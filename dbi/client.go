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
	StopLong()
    Stop()
}

type FilterFunc func(string) bool
type QueryResult struct {
	ID		string	// Short unique ID created by makeID() function
	FullID	string	// Full object identifier includes prefix, hostname and found path
}

func NewClient(dbCfg *DBConfig) (DBClient, error) {
	// Initiate database client
	return newDBClient(dbCfg)
}
