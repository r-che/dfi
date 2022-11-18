//go:build dbi_redis
package dbi

import (
	"github.com/r-che/dfi/dbi/redis"
	"github.com/r-che/dfi/types/dbms"
)

func NewClientController(dbCfg *dbms.DBConfig) (dbms.ClientController, error) {
	// Initiate controller client
	return redis.NewClient(dbCfg)
}

func NewClient(dbCfg *dbms.DBConfig) (dbms.Client, error) {
	// Initiate database client
	return redis.NewClient(dbCfg)
}
