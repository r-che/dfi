//go:build dbi_mongo
package dbi

import (
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/dbi/mongo"
)

func NewClientController(dbCfg *dbms.DBConfig) (dbms.ClientController, error) {
	// Initiate controller client
	return mongo.NewClient(dbCfg)
}

func NewClient(dbCfg *dbms.DBConfig) (dbms.Client, error) {
	// Initiate database client
	return mongo.NewClient(dbCfg)
}
