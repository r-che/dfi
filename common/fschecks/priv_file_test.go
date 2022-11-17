//go:build linux
package fschecks

import (
	"fmt"
	"testing"
	"os"
	"io/fs"
	"time"
	"syscall"
	"errors"
	"path/filepath"
)

var tests = []struct{
	ownerOk	bool
	modeOk	bool
	wantErr	error
} {
	{	// Correct owner and access mode 0600
		ownerOk:	true,
		modeOk:		true,
	},
	{	// Incorrect owner/mode
		wantErr:	&ErrOwner{OwnerError{errors.New("incorrect owner")}},
	},
	{	// Incorrect mode
		ownerOk:	true,
		wantErr:	&ErrPerm{OwnerError{errors.New("incorrect access mode")}},
	},
}

func TestPrivOwnership(t *testing.T) {
	for testN, test := range tests {
		// Get the name of test file
		testFile, needRm, err := getTestFile(test.ownerOk, test.modeOk)
		if err != nil {
			t.Errorf("%v", err)
			// Go to the next test
			continue
		} else if needRm {
			// Cleanup temporary file on exit
			defer os.Remove(testFile)
		}

		// Run function
		err = PrivOwnership(testFile)
		switch {
		case err == nil:
			// Check that no errors wanted
			if test.wantErr == nil {
				// Ok, go to the next test
				continue
			}
			t.Errorf("[%d] case successed, but must not, want error %T (%v)", testN, test.wantErr, test.wantErr)

		case fmt.Sprintf("%T", err) == fmt.Sprintf("%T", test.wantErr):
			// Success, expected error

		default:
			t.Errorf("unexpected error: got - %T (%v), want - %T (%v)", err, err, test.wantErr, test.wantErr)
		}
	}
}

func TestPrivOwnershipStatFail(t *testing.T) {
	// Create a test file with random name
	testFile, needRm, err := getTestFile(true, true)
	if err != nil {
		t.Errorf("cannot create temporary file: %v", err)
	}
	if needRm {
		defer os.Remove(testFile)
	}

	// Create non-existing filename
	nxFile := filepath.Join(testFile, "this-file-does-not-exist")

	// Call PrivOwnership
	err = PrivOwnership(nxFile)
	switch {
	case err == nil:
		t.Errorf("case successed, but must not - returned success for non-existing file %q", nxFile)
	case errors.As(err, new(*fs.PathError)):
		// Success, expected error
	default:
		t.Errorf("case returned unexpected error type %T (%v), want *fs.PathError", err, err)
	}
}

func TestOwnerError(t *testing.T) {
	const testEO = "test OwnerError error"
	err := &OwnerError{errors.New(testEO)}
	if err.Error() != testEO {
		t.Errorf("OwnerError.String() returned %q, want - %q", testEO, testEO)
	}
}

//
// system stat() call testing
//
type fakeFI struct{}
func (ffi *fakeFI) Name() string { return "fake-file" }
func (ffi *fakeFI) Size() int64 { return 0 }
func (ffi *fakeFI) Mode() fs.FileMode { return 0 }
func (ffi *fakeFI) ModTime() time.Time { return time.Time{} }
func (ffi *fakeFI) IsDir() bool { return false }
func (ffi *fakeFI) Sys() any { return nil }

func Test_sysStat(t *testing.T) {
	_, err := sysStat(&fakeFI{})
	switch {
	case errors.As(err, new(*ErrGetOwner)):
		// Success, expected error
	case err == nil:
		t.Errorf("no errors on fake interface")
		t.FailNow()
	default:
		t.Errorf("want retrieving ownership error, got - %T (%v)", err, err)
	}
}

func Test_sysStat_fail(t *testing.T) {
	testFile, needRm, err := getTestFile(false, false)
	if err != nil {
		t.Errorf("%v", err)
		t.FailNow()
	} else if needRm {
		// Cleanup temporary file on exit
		defer os.Remove(testFile)
	}

	// Replace sysStat by function that always retunrs error
	sysStat = func(fi os.FileInfo) (*syscall.Stat_t, error) {
		return nil, &ErrGetOwner{OwnerError{fmt.Errorf("negative case testing")}}
	}

	// Now, test ownership of this file
	err = PrivOwnership(testFile)
	switch {
	case err == nil:
		t.Errorf("PrivOwnership does not catch invalid file ownership")
	case errors.As(err, new(*ErrGetOwner)):
		// Success, expected error
	default:
		t.Errorf("want the incorrect ownership error, got - %T (%v)", err, err)
	}
}

//
// Auxiliary functions
//

//nolint:cyclop // Simplifying the code will not make it clearer
func getTestFile(ownerOk, modeOk bool) (string, bool, error) {
	// Need to get current effective user ID
	uid := os.Geteuid()
	if uid < 0 {
		return "", false, fmt.Errorf("cannot get UID of current process - unsupported value %d", uid)
	}

	// If requested incorrect owner and I'm not root
	if uid != 0 && !ownerOk {
		// Return file that always has root:root & 0644 perms
		return "/etc/passwd", false, nil
	}

	// Need to create temporary file
	f, err := os.CreateTemp("", ".go-test-rche.")
	if err != nil {
		return "", false, fmt.Errorf("cannot create temporary file: %w", err)
	}
	// Close immediately, we do not needed it open
	if err := f.Close(); err != nil {
		return "", false, fmt.Errorf("cannot close temporary file: %w", err)
	}

	if uid == 0 && !ownerOk {
		// Change the owner/group to "random" values
		if err := os.Chown(f.Name(), 1825, 1825); err != nil {
			return "", false, fmt.Errorf("cannot cange owner of created temporary file: %w", err)
		}
	}

	if modeOk {
		// Set correct access mode - only owner can read/write
		if err := os.Chmod(f.Name(), 0o600); err != nil {
			return "", false, fmt.Errorf("cannot set correct mode to the test file %q: %w", f.Name(), err)
		}
	} else {
		// Set incorrect access mode - all can read
		if err := os.Chmod(f.Name(), 0o644); err != nil {
			return "", false, fmt.Errorf("cannot set incorrect mode to the test file %q: %w", f.Name(), err)
		}
	}

	return f.Name(), true, nil
}
