package dbi

import (
	"fmt"
	"crypto/sha1"

	"github.com/r-che/dfi/types"
)

// Standard database connection configuration
type DBConfig struct {
	HostPort	string

	User		string
	Password	string
}

// Supported operators on database
type DBOperator int
const (
	Update = DBOperator(iota)
	Delete
)
func (dbo DBOperator) String() string {
	switch dbo {
		case Update: return "Update"
		case Delete: return "Delete"
		default:
			panic(fmt.Sprintf("Unsupported database operation %d", dbo))
	}
}

// Operation with attached data that (if exists) should be inserted into DB
type DBOperation struct {
	Op DBOperator
	ObjectInfo *types.FSObject
}

// Type of function that reads database operations from wathers to apply them to database
type DBContrFunc func()

// Database object fields
const (
	FieldID = "id"			// Unique object identifier (sha1 of found path)
	FieldName = "name"		// File name (w/o full path)
	FieldFPath = "fpath"	// Where object was found, may include symbolic links
	FieldRPath = "rpath"	// Where object really placed
	FieldType = "type"		// Regular file, directory, symbolic link
	FieldSize = "size"		// Size of object in bytes, if applicable
	FieldChecksum = "csum"	// Message digest, if enabled by indexer settings
)

// makeID makes the identifier (most unique) for a particular filesystem object
func makeID(fso *types.FSObject) string {
	// Use found path as value to generate the identifier
	return fmt.Sprintf("%x", sha1.Sum([]byte(fso.FPath)))
}
