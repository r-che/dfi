package mongo

import (
	"fmt"

//	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

//	"github.com/r-che/log"
)

//
// CLI/Web/REST clients interface
//
func (mc *MongoClient) Query(qa *dbms.QueryArgs, retFields []string) (qr dbms.QueryResults, err error) {
	return nil, fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) QueryAIIIds(qa *dbms.QueryArgs) (ids []string, err error) {
	return nil, fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) GetObjects(ids, retFields []string) (qr dbms.QueryResults, err error) {
	return nil, fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) GetAIIs(ids, retFields  []string) (qr dbms.QueryResultsAII, err error) {
	return nil, fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) GetAIIIds(withFields []string) (ids []string, err error) {
	return nil, fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) ModifyAII(dbms.DBOperator, *dbms.AIIArgs, []string, bool) (tagsUpdated, descrsUpdated int64, err error) {
	return -1, -1, fmt.Errorf("Not implemented")	// TODO
}
