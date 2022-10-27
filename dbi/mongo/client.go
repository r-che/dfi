package mongo

import (
	"fmt"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//
// CLI/Web/REST clients interface
//

func (mc *MongoClient) Query(qa *dbms.QueryArgs, retFields []string) (dbms.QueryResults, error) {
	filter := makeFilter(qa)
	// TODO
	log.D("Prepared MongoDB BSON query filter: %v\n", filter) // XXX Query may be too long


	// Create list of requested fields
	fields := bson.D{}
	for _, field := range retFields {
		fields = append(fields, bson.E{Key: field, Value: 1})

	}

	// Find request options
	opts := options.Find()

	// List of fields required to create object key
	keyFields := []string{dbms.FieldHost, dbms.FieldFPath}

	// Is requested fields not empty?
	if len(fields) != 0 {
		// Check for some field required to creation of object key was not added
		if kfSet := tools.NewStrSet(keyFields...).Del(retFields...); len(*kfSet) != 0 {
			// Add these fields to request
			for _, field := range kfSet.List() {
				fields = append(fields, bson.E{Key: field, Value: 1})
			}
		}

		// Set projection to the find request options
		opts.SetProjection(fields)
	}

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(ObjsCollection)

	// Run query
	cursor, err := coll.Find(mc.Ctx, filter, opts)
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

	// Output result
	qr := make(dbms.QueryResults, dbms.ExpectedMaxResults)

	// Required fields that must present in the each result item
	rqFields := append([]string{MongoIDField}, keyFields...)

	// Make a list of results
	for cursor.Next(mc.Ctx) {
		var item map[string]any
		// Try to decode next cursor value to the item
		if err := cursor.Decode(&item); err != nil {
			log.E("(MongoCli:Query) cannot decode cursor item: %w", err)
			// Break cursor loop
			break
		}

		// Check for all required fields are present
		for _, field := range rqFields {
			v, ok := item[field]
			if !ok {
				log.E("(MongoCli:Query) Skip returned result without" +
					" required field %q data: %#v", field, item)
				continue
			}
			// Check that value is string
			if _, ok := v.(string); !ok {
				log.E("(MongoCli:Query) Skip returned result with non-string value of" +
					" required field %q, value: %#v (%T)", field, v, v)
				continue
			}
		}

		// Replace MongoIDField by dbms.FieldID in the result
		item[dbms.FieldID] = item[MongoIDField]	// TODO Need to replace Find() by collection.Aggregate to avoid this renaming
		delete(item, MongoIDField)

		// Save result
		qr[types.ObjKey{
			Host: item[dbms.FieldHost].(string),
			Path: item[dbms.FieldFPath].(string),
		}] = item
	}

	// Check for deep search required
	if qa.DeepSearch {
		// TODO Do search with text index, probably it should replace default search instead of add some data
	}

	return qr, nil
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
