package dbi

import (
	//"context"
	//"sync"

	"github.com/r-che/dfi/types"

	//"github.com/r-che/log"
)

type DBClient interface {
    UpdateObj(*types.FSObject) error
    DeleteObj(*types.FSObject) error
    Delete(string) error
    Commit() (int64, int64, error)
	LoadHostPaths() ([]string, error)
    Stop()
}

func NewClient(dbCfg *DBConfig) (DBClient, error) {
	// Initiate database client
	return newDBClient(dbCfg)
}
