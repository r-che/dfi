package fswatcher

import (
	"fmt"
	"os"
	"io/fs"
	"crypto/sha1"
	"io"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/dfiagent/internal/cfg"

	"github.com/r-che/log"
)

func getObjectInfo(name string) (*types.FSObject, error) {
	// Get agent configuration
	c := cfg.Config()

	// Get object information to update data in DB
	oi, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}

	// Fill filesystem object
	fso := types.FSObject {
		Name:		oi.Name(),
		FPath:		name,
		Size:		oi.Size(),
		MTime:		oi.ModTime().Unix(),
		Checksum:	"",
	}

	switch {
	case oi.Mode() & fs.ModeSymlink != 0:
		// Resolve symbolic link value
		if fso.RPath, err = os.Readlink(name); err != nil {
			log.W("Cannot resolve symbolic link object %q to real path: %v", name, err)
		}

		// Assign proper type
		fso.Type = types.ObjSymlink
		// Continue handling
	case oi.IsDir():
		// Assign proper type
		fso.Type = types.ObjDirectory
	case oi.Mode().IsRegular():
		// Assign proper type
		fso.Type = types.ObjRegular

		// Get checksum but only if enabled
		if c.CalcSums {
			if err = calcSum(&fso, c.MaxSumSize); err != nil {
				log.W("Checksum calculation problem: %v", err)
				// Set stub to signal checksum calculation error
				fso.Checksum = types.CsErrorStub
			}
		}

	// Unsupported filesystem object type
	default:
		return nil, errUnsupportedType
	}

	return &fso, nil
}

func calcSum(fso *types.FSObject, maxSize int64) error {
	if maxSize != 0 && fso.Size > maxSize {
		// Set stub because file is too large to calculate checksum
		fso.Checksum = types.CsTooLarge

		return nil
	}

	log.D("Checksum of %q - calculating...", fso.FPath)

	// Open file to calculate checksum of its content
	f, err := os.Open(fso.FPath)
	if err != nil {
		// Set stub to signal checksum calculation error
		fso.Checksum = types.CsErrorStub

		return err
	}
	defer f.Close()

	// Hash object to calculate sum
	hash := sha1.New()
	if _, err := io.Copy(hash, f); err != nil {
		return err
	}

	log.D("Checksum of %q - done", fso.FPath)

	fso.Checksum = fmt.Sprintf("%x", hash.Sum(nil))

	// OK
	return nil
}
