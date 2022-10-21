package dbms

import (
	"fmt"
	"strings"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/common/parse"
)

// Agent client interface
type ClientController interface {
	LoadHostPaths(filter MatchStrFunc) (paths []string, err error)
	UpdateObj(fso *types.FSObject) error
	DeleteObj(fso *types.FSObject) error
	Commit() (updated, deleted int64, err error)

	// Management methods
	SetReadOnly(ro bool)
	TermLong()
	Stop()
}

// CLI/Web/REST clients interface
type Client interface {
	// Search methods
	Query(qa *QueryArgs, retFields []string) (qr QueryResults, err error)
	QueryAIIIds(qa *QueryArgs) (ids []string, err error)

	// Get objects/identifiers without search
	GetObjects(ids, retFields []string) (qr QueryResults, err error)
	GetAIIs(ids, retFields  []string) (qr QueryResultsAII, err error)
	GetAIIIds(withFields []string) (ids []string, err error)

	// Modification of AII
	ModifyAII(DBOperator, *AIIArgs, []string, bool) (tagsUpdated, descrsUpdated int64, err error)
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
func (aa *AIIArgs) SetTagsStr(tagsStr string) error {
	aa.Tags = []string{}
	if err := parse.StringsSet(&aa.Tags, AIIFieldTags, tagsStr); err != nil {
		return err
	}
	// Trim spaces from each tag
	for i := range aa.Tags {
		aa.Tags[i] = strings.TrimSpace(aa.Tags[i])
	}
	return nil
}
func (aa *AIIArgs) SetDescr(descr string) {
	aa.Descr = strings.TrimSpace(descr)
}
func (aa *AIIArgs) SetFieldStr(field, value string) error {
	var err error

	switch field {
	case AIIFieldDescr:
		aa.SetDescr(value)
	case AIIFieldTags:
		err = aa.SetTagsStr(value)
	default:
		return fmt.Errorf("unknown AII field %q (value: %#v)", field, value)
	}

	return err
}

type MatchStrFunc func(string) bool

// Map to return query results indexed host + found path
type QueryResults map[types.ObjKey] map[string]any
// Map to return AII query results
type QueryResultsAII map[string]*AIIArgs

// Database object fields
const (
	FieldID = "id"			// Unique object identifier (sha1 of found path)
	FieldHost = "host"		// Host where the object was found
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
		// XXX Do not forget to update test when update this set of fields
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
