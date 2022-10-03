//go:build dbi_redis
package dbi

import (
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/dbi/redis"
)

func NewClient(dbCfg *dbms.DBConfig) (dbms.Client, error) {
	// Initiate database client
	return redis.NewClient(dbCfg)
}
