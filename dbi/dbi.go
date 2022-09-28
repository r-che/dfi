package dbi

import (
	"fmt"
	"crypto/sha1"

	"github.com/r-che/dfi/types"
)

// Standard database connection configuration
type DBConfig struct {
	// Connection information
	HostPort	string
	CliHost		string	// Client hostname

	// Database specific information
	ID			string			// Database identifier - name, number, etc...
	PrivCfg		map[string]any	// Private configuration loaded from JSON
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

type DBChan chan []*DBOperation

// Database object fields
const (
	FieldID = "id"			// Unique object identifier (sha1 of found path)
	FieldHost = "host"		// Hosw where the object was found
	FieldName = "name"		// File name (w/o full path)
	FieldFPath = "fpath"	// Where object was found, may include symbolic links
	FieldRPath = "rpath"	// Where object really placed
	FieldType = "type"		// Regular file, directory, symbolic link
	FieldSize = "size"		// Size of object in bytes, if applicable
	FieldMTime = "mtime"	// Object modifications time
	FieldChecksum = "csum"	// Message digest, if enabled by indexer settings
)

// Additional information item (AII) fields
const (
	AIIFieldTags	=	"tags"
	AIIFieldDescr	=	"descr"
	AIIFieldOID		=	"oid"

	AIIAllTags		=	"ALL"
	AIIDelDescr		=	"\u0000\u0000DELETE DESCRIPTION\u0000\u0000"
)

// makeID makes the identifier (most unique) for a particular filesystem object
func makeID(host string, fso *types.FSObject) string {
	// Use found path as value to generate the identifier
	return fmt.Sprintf("%x", sha1.Sum([]byte(host + ":" + fso.FPath)))
}
