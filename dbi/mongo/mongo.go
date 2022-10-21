package mongo

import (
	"fmt"

//	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

//	"github.com/r-che/log"
)

type MongoClient struct {

}

func NewClient(dbCfg *dbms.DBConfig) (*MongoClient, error) {
	return nil, fmt.Errorf("mongo.NewClient() not implemented")
}
