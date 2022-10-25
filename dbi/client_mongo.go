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
	// Disable pinging of database on client creation,
	// because client usually executes commands immediately
	mongo.DisableStartupPing()

	// Initiate database client
	return mongo.NewClient(dbCfg)
}
