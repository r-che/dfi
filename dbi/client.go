package dbi

import (
	"github.com/r-che/dfi/types"
)

type DBClient interface {
    UpdateObj(*types.FSObject) error
    DeleteObj(*types.FSObject) error
    Commit() (int64, int64, error)
	LoadHostPaths(FilterFunc) ([]string, error)
	Query(*QueryArgs, []string) (QueryResults, error)
	ModifyAII(DBOperator, *AIIArgs, []string, bool) error
	StopLong()
    Stop()
}

type FilterFunc func(string) bool
// Map to return query results indexed host + found path
// Values of this map represents key-value pairs with requested objects properties
type QRKey struct {
	Host string
	Path string
}
type QueryResults map[QRKey] map[string]any

// Additional information item (AII) arguments
type AIIArgs struct {
	Tags	[]string
	Descr	string
	NoNL	bool
}

func NewClient(dbCfg *DBConfig) (DBClient, error) {
	// Initiate database client
	return newDBClient(dbCfg)
}
