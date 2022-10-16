//go:build linux
package fschecks

import (
	"fmt"
	"os"
	"syscall"
)

type ErrOwnership struct {
	err error
}
func (e *ErrOwnership) Error() string {
	return e.err.Error()
}
type ErrGetOwner struct { ErrOwnership }
type ErrOwner struct { ErrOwnership }
type ErrPerm struct { ErrOwnership }

func PrivOwnership(file string) error {
	// Check ownership and permissions of the private file:
	// * must belong to the current user
	// * must be readable only by owner
	// * must be writable only by owner
	uid := os.Getuid()
	fi, err := os.Stat(file)
	if err != nil {
		return err
	}

	// Get system dependend struct stat, see man stat(2)
	stat, err := sysStat(fi)
	if err != nil {
		return err
	}

	// Check ownership
	if uint32(uid) != stat.Uid {
		return &ErrOwner{ErrOwnership{fmt.Errorf(
			"UID of the user running the application is %d, but the UID of the owner of the file %q is %d - " +
			"refusing to use this file because of a security breach, the file must belong to the application user",
			uid, file, stat.Uid)}}
	}

	// Check the file access mode
	if mode := fi.Mode().Perm(); mode & 0o066 != 0 {
		return &ErrPerm{ErrOwnership{fmt.Errorf(
			"file %q must NOT be read/write accessible by the group/all users, " +
			"only the application user must have read access to it, current permission mode is: %o",
			file, mode)}}
	}

	// OK
	return nil
}

// Get system dependend struct stat, see man stat(2)
var sysStat = func(fi os.FileInfo) (*syscall.Stat_t, error) {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, &ErrGetOwner{ErrOwnership{fmt.Errorf(
				"fails to retrieve the UID of the owner of %q from %T structure," +
				" possible the platform is not supported", fi.Name(), fi)}}
	}

	return stat, nil
}
