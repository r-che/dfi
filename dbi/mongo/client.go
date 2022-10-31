package mongo

import (
	"fmt"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

//
// CLI/Web/REST clients interface
//

func (mc *MongoClient) Query(qa *dbms.QueryArgs, retFields []string) (dbms.QueryResults, error) {
	// By default run full text search
	log.D("(MongoCli:Query) Running full-text search with {$text: { $search: … }} ...")
	qr, err := mc.runSearch(MongoObjsColl, qa, makeFilterFullTextSearch(qa), retFields)
	if err != nil {
		return qr, fmt.Errorf("(MongoCli:Query) full-text search failed: %w", err)
	}

	// Check for deep search is not required
	if !qa.DeepSearch {
		// Return resulst
		return qr, nil
	}

	//
	// Run additional regex-based search
	//

	log.D("(MongoCli:Query) Running regex based deep search ...")
	qrDeep, err := mc.runSearch(MongoObjsColl, qa, makeFilterRegexSP(qa), retFields)
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:Query) deep (regex-based) search failed with: %w", err)
	}

	// Save number of found items before merging
	n := len(qr)

	// Merge qrDeep with qr
	for k, v := range qrDeep {
		// Check for key already exists
		if _, ok := qr[k]; ok {
			log.D("(MongoCli:Query) Object already found: %v", k)
			continue
		}
		// Update existing query results
		qr[k] = v
	}

	log.D("(MongoCli:Query) Total %d additional records were found using deep (regex-based) search", len(qr) - n)

	return qr, nil
}

func (mc *MongoClient) runSearch(collName string, qa *dbms.QueryArgs, spFilter bson.D, retFields []string) (dbms.QueryResults, error) {
	// Make filter for default regexp-based search - join the search
	// phrases and idenfifiers (if any) with the arguments filter
	filter := joinFilters(useAnd,
		// Join all provided conditions (search phrases and idenifiers) by logical OR
		joinByOr(
			// Merge search phrases with identifiers (if provided)
			mergeIdsWithSPs(qa, spFilter),
		),

		// Join with the arguments filter
		makeFilterByArgs(qa),
	)

	// TODO
	log.D("(MongoCli:runSearch) Prepared Mongo filter for search in %q: %v", collName, filter)	// XXX Raw query may be too long

	qr, err := mc.aggregateSearch(collName, filter, retFields)
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:runSearch) %w", err)
	}

	return qr, nil
}

func (mc *MongoClient) aggregateSearch(collName string, filter bson.D, retFields []string) (dbms.QueryResults, error) {
	// Filter-replace pipeline
	filRepPipeline := mongo.Pipeline{
		bson.D{{ `$match`, filter}},	// apply filter
	}

	// Create list of requested fields
	fields := bson.D{}
	for _, field := range retFields {
		fields = append(fields, bson.E{Key: field, Value: 1})
	}

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

		// Add $project stage to pipeline to to set the requested fields set
		filRepPipeline = append(filRepPipeline, bson.D{{ `$project`, fields }})
	}

	// Add $addFields stage to replace field name _id by id
	filRepPipeline = append(filRepPipeline, bson.D{{`$addFields`, bson.D{
		{MongoIDField, `$REMOVE`},
		{dbms.FieldID, `$` + MongoIDField},
	}}})

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(collName)

	// Run aggregated query
	cursor, err := coll.Aggregate(mc.Ctx, filRepPipeline)
	if err != nil {
		// Unexpected error
		return nil, fmt.Errorf("(MongoCli:aggregateSearch) aggregate on %s.%s with filter %v failed: %v",
			coll.Database().Name(), coll.Name(), filter, err)
	}
	defer func() {
		if err := cursor.Close(mc.Ctx); err != nil {
			log.E("(MongoCli:aggregateSearch) cannot close cursor: %v", err)
		}
	}()

	// Required fields that must present in the each result item
	rqFields := append([]string{dbms.FieldID}, keyFields...)

	// Output result
	qr := make(dbms.QueryResults, dbms.ExpectedMaxResults)

	// Make a list of results
	for cursor.Next(mc.Ctx) {
		var item map[string]any
		// Try to decode next cursor value to the item
		if err := cursor.Decode(&item); err != nil {
			log.E("(MongoCli:aggregateSearch) cannot decode cursor item: %w", err)
			// Break cursor loop
			break
		}

		// Check for all required fields are present
		for _, field := range rqFields {
			v, ok := item[field]
			if !ok {
				log.E("(MongoCli:aggregateSearch) Skip returned result without" +
					" required field %q data: %#v", field, item)
				continue
			}
			// Check that value is string
			if _, ok := v.(string); !ok {
				log.E("(MongoCli:aggregateSearch) Skip returned result with non-string value of" +
					" required field %q, value: %#v (%T)", field, v, v)
				continue
			}
		}

		// Save result
		qr[types.ObjKey{
			Host: item[dbms.FieldHost].(string),
			Path: item[dbms.FieldFPath].(string)},
		] = item
	}

	return qr, nil
}

func (mc *MongoClient) QueryAIIIds(qa *dbms.QueryArgs) (ids []string, err error) {
	return nil, fmt.Errorf("QueryAIIIds - Not implemented")	// TODO
}

func (mc *MongoClient) GetObjects(ids, retFields []string) (dbms.QueryResults, error) {
	qr, err := mc.runSearch(MongoObjsColl, &dbms.QueryArgs{}, makeFilterIDs(ids), retFields)
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:GetObjects) regex search failed with: %w", err)
	}

	// Success
	return qr, nil
}

func (mc *MongoClient) GetAIIs(ids, retFields  []string) (qr dbms.QueryResultsAII, err error) {
	return nil, fmt.Errorf("GetAIIs - Not implemented")	// TODO
}
func (mc *MongoClient) GetAIIIds(withFields []string) (ids []string, err error) {
	return nil, fmt.Errorf("GetAIIIds - Not implemented")	// TODO
}
