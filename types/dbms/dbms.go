package dbms

import (
	"fmt"
	"github.com/r-che/dfi/types"
)

type Client interface {
    UpdateObj(*types.FSObject) error
    DeleteObj(*types.FSObject) error
    Commit() (int64, int64, error)

	ModifyAII(DBOperator, *AIIArgs, []string, bool) (int64, int64, error)

	LoadHostPaths(FilterFunc) ([]string, error)
	Query(*QueryArgs, []string) (QueryResults, error)
	GetObjects([]string, []string) (QueryResults, error)
	GetAIIIds(withFields []string) ([]string, error)
	GetAIIs([]string, []string) (map[string]map[string]string, error)
	QueryAIIIds(qa *QueryArgs) ([]string, error)

	StopLong()
    Stop()
}

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

// Additional information item (AII) arguments
type AIIArgs struct {
	Tags	[]string
	Descr	string
	NoNL	bool
}

type FilterFunc func(string) bool

// Map to return query results indexed host + found path
type QueryResults map[types.ObjKey] map[string]any

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
// UVObjFields returns user valuable object fields
func UVObjFields() []string {
	return []string {
		FieldID,
		FieldRPath,
		FieldType,
		FieldSize,
		FieldMTime,
		FieldChecksum,
	}
}

// Additional information item (AII) fields
const (
	AIIFieldTags	=	"tags"
	AIIFieldDescr	=	"descr"
	AIIFieldOID		=	"oid"

	AIIAllTags		=	"ALL"
	AIIDelDescr		=	"\u0000\u0000DELETE DESCRIPTION\u0000\u0000"
)
// UVAIIFields returns user valuable AII fields
func UVAIIFields() []string {
	return []string {
		AIIFieldTags,
		AIIFieldDescr,
	}
}
