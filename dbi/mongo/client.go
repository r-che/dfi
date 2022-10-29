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
	// Output result
	qr := make(dbms.QueryResults, dbms.ExpectedMaxResults)

	// By default run simple regex-based search
	qr, err := mc.regexSearch(qa, retFields, qr)
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:Query) regex search failed with: %w", err)
	}

	// Check for deep search required
	if qa.DeepSearch {
		// Do additional standard SCAN search
		log.D("(MongoCli:Query) Running deep search - full-text search with {$text: { $search: … }} ...")
		n, err := mc.fullTextSearch(qa, retFields, qr)
		if err != nil {
			return qr, fmt.Errorf("(MongoCli:Query) full-text search failed: %w", err)
		}
		log.D("(MongoCli:Query) Total of %d records were found with a deep (SCAN) search", n)
	}

	return qr, nil
}

func (mc *MongoClient) regexSearch(qa *dbms.QueryArgs, retFields []string, qr dbms.QueryResults) (dbms.QueryResults, error) {
	// Make filter for default regexp-based search - join the search
	// phrases and idenfifiers (if any) with the arguments filter
	filter := joinFilters(useAnd,
		// Join all provided conditions (search phrases and idenifiers) by logical OR
		joinByOr(
			// Merge search phrases with identifiers (if provided)
			mergeIdsWithSPs(qa, makeFilterRegexSP(qa)),
		),

		// Join with the arguments filter
		makeFilterByArgs(qa),
	)

	// TODO
	log.D("(MongoCli:regexSearch) Prepared Mongo query filter: %v", filter)	// XXX Raw query may be too long

	if err := mc.aggregateSearch(filter, retFields, qr); err != nil {
		return qr, fmt.Errorf("(MongoCli:regexSearch) %w", err)
	}

	return qr, nil
}

func (mc *MongoClient) fullTextSearch(qa *dbms.QueryArgs, retFields []string, qrTop dbms.QueryResults) (int, error) {

	return 0, fmt.Errorf("Not implemented") // TODO
}

func (mc *MongoClient) aggregateSearch(filter bson.D, retFields []string, qr dbms.QueryResults) error {
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
	coll := mc.c.Database(mc.Cfg.ID).Collection(ObjsCollection)

	// Run aggregated query
	cursor, err := coll.Aggregate(mc.Ctx, filRepPipeline)
	if err != nil {
		// Unexpected error
		return fmt.Errorf("(MongoCli:aggregateSearch) aggregate on %s.%s with filter %v failed: %v",
			coll.Database().Name(), coll.Name(), filter, err)
	}
	defer func() {
		if err := cursor.Close(mc.Ctx); err != nil {
			log.E("(MongoCli:aggregateSearch) cannot close cursor: %v", err)
		}
	}()

	// Required fields that must present in the each result item
	rqFields := append([]string{dbms.FieldID}, keyFields...)

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
			Path: item[dbms.FieldFPath].(string),
		}] = item
	}

	return nil
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
