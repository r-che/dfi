package mongo

import (
	"fmt"

//	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

	"github.com/r-che/log"

	//"go.mongodb.org/mongo-driver/mongo/options"
)

//
// CLI/Web/REST clients interface
//

func (mc *MongoClient) Query(qa *dbms.QueryArgs, retFields []string) (qr dbms.QueryResults, err error) {
	filter := makeFilter(qa)
	// TODO
	log.D("Prepared MongoDB BSON query filter: %v\n", filter) // XXX Query may be too long

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(ObjsCollection)

	// Run query
	cursor, err := coll.Find( mc.Ctx, filter, /*options.Find().SetReturnKey(true)*/) // TODO Fix options - need to set requested set of fields
	if err != nil {
		// Unexpected error
		return nil, fmt.Errorf("(MongoCli:Query) find on %s.%s with filter %v failed: %v",
			coll.Database().Name(), coll.Name(), filter, err)
	}
	defer func() {
		if err := cursor.Close(mc.Ctx); err != nil {
			log.E("(MongoCli:Query) cannot close cursor: %v", err)
		}
	}()

	// Make a list of results
	for cursor.Next(mc.Ctx) {
		var item map[string]any
		// Try to decode next cursor value to the item
		if err := cursor.Decode(&item); err != nil {
			log.E("(MongoCli:Query) cannot decode cursor item: %w", err)
			// Break cursor loop
			break
		}

		fmt.Printf("OK - %s %s:%s\n", item["_id"], item["host"], item["fpath"])	// TODO
	}

	fmt.Println()
	return nil, fmt.Errorf("Query - Not implemented")	// TODO
}


func (mc *MongoClient) QueryAIIIds(qa *dbms.QueryArgs) (ids []string, err error) {
	return nil, fmt.Errorf("QueryAIIIds - Not implemented")	// TODO
}
func (mc *MongoClient) GetObjects(ids, retFields []string) (qr dbms.QueryResults, err error) {
	return nil, fmt.Errorf("GetObjects - Not implemented")	// TODO
}
func (mc *MongoClient) GetAIIs(ids, retFields  []string) (qr dbms.QueryResultsAII, err error) {
	return nil, fmt.Errorf("GetAIIs - Not implemented")	// TODO
}
func (mc *MongoClient) GetAIIIds(withFields []string) (ids []string, err error) {
	return nil, fmt.Errorf("GetAIIIds - Not implemented")	// TODO
}
func (mc *MongoClient) ModifyAII(dbms.DBOperator, *dbms.AIIArgs, []string, bool) (tagsUpdated, descrsUpdated int64, err error) {
	return -1, -1, fmt.Errorf("ModifyAII - Not implemented")	// TODO
}
