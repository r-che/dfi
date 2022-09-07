package dbi

import (
	//"context"
	//"sync"

	"github.com/r-che/dfi/types"

	//"github.com/r-che/log"
)

type DBClient interface {
    Update(*types.FSObject) error
    Delete(*types.FSObject) error
    Commit() (int64, int64, error)
    Stop()
}

func NewClient(dbCfg *DBConfig) (DBClient, error) {
	// Initiate database client
	return newDBClient(dbCfg)
}
