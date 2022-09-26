package dbi

import (
	"github.com/r-che/dfi/types"
)

type DBClient interface {
    UpdateObj(*types.FSObject) error
    DeleteObj(*types.FSObject) error
    Delete(string) error
    Commit() (int64, int64, error)
	LoadHostPaths(FilterFunc) ([]string, error)
	Query(*QueryArgs, []string) (QueryResults, error)
	StopLong()
    Stop()
}

type FilterFunc func(string) bool
// Map to return query results indexed  by full object identifier in format: hostname:/foun/path
// Values of this map represents key-value pairs with requested objects properties
type QueryResults map[string]map[string]any

func NewClient(dbCfg *DBConfig) (DBClient, error) {
	// Initiate database client
	return newDBClient(dbCfg)
}
