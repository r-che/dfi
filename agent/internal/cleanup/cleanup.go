package cleanup

import (
	"fmt"
	"strings"
	"errors"
	"os"
	"io/fs"

	"github.com/r-che/log"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/agent/internal/cfg"
)

func Run() error {
	// Create new database client
	dbc, err := dbi.NewClientController(&cfg.Config().DBCfg)
	if err != nil {
		return err
	}

	log.I("(Cleanup) Started")

	// Load application configuration
	c := cfg.Config()

	// Counters of not configured record and stale (not existing on FS) records
	nc, nx := 0, 0

	// Make a function to match records that should be deleted from DB
	match := func(path string) bool {
		// Iterate through list of paths configured for indexing
		for _, confPath := range c.IdxPaths {
			if strings.HasPrefix(path, confPath) {
				// OK, this path inside of configured directories
				goto fsCheck
			}
		}

		// This path is outside of configured directories, append to delete
		log.D("(Cleanup) Path %q does not belong to configured directories (%v)", path, c.IdxPaths)
		nc++

		// Skip filesystem check because it does not make sense,
		// just return true because this path SHOULD BE DELETED
		return true

		fsCheck:
		// Check path for existing
		_, err := os.Lstat(path)
		if err == nil {
			// OK, path exists, check next => should NOT be deleted
			return false
		}

		// Check a type of the errror
		if errors.Is(err, fs.ErrNotExist) {
			log.D("(Cleanup) Path %q does not exist on the local filesystem", path)
			nx++

			// Path is not exist, SHOULD BE DELETED from DB
			return true
		}

		log.E("(Cleanup) Cannot check existing of path %q: %v", path, err)
		// Some unexpected error, log it, DO NOT delete the record
		return false
	}

    // Load from database all objects that belong to the current host
	toDel, err := dbc.LoadHostPaths(match)
	if err != nil {
		return fmt.Errorf("(Cleanup) cannot load list of objects paths belong to this host: %v", err)
	}

	// Check for stale data
	if nc + nx == 0 {
		log.I("(Cleanup) Nothing to clean")
		return nil
	}

	log.I("(Cleanup) %d not configured and %d non-existing records found", nc, nx)

	// Delete outdated paths
	for _, path := range toDel {
		if err := dbc.DeleteObj(&types.FSObject{FPath: path}); err != nil {
			log.E("(Cleanup) Cannot delete object for path %q: %v", path, err)
		}
	}

	// Commit deletion
	if _, deleted, err := dbc.Commit(); err != nil {
		log.E("(Cleanup) Fail to delete objects from DB: %v", err)
	} else {
		log.I("(Cleanup) Cleaned up %d objects from DB", deleted)
	}

	// OK
	return nil
}
