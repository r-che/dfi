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

func (mc *Client) Query(qa *dbms.QueryArgs, retFields []string) (dbms.QueryResults, error) {
	searchType := tools.Tern(len(qa.SP) == 0, "only arguments-based", "full-text")

	log.D("(MongoCli:Query) Running %s search ...", searchType)
	qr, err := mc.runSearch(MongoObjsColl, qa, filterMakeFullTextSearch(qa), retFields)
	if err != nil {
		return qr, fmt.Errorf("(MongoCli:Query) %s search failed: %w", searchType, err)
	}

	// Check for deep search is not required or not possible (without search phrases)
	if !qa.DeepSearch || len(qa.SP) == 0 {
		// Return results
		return qr, nil
	}

	//
	// Run additional regex-based search
	//

	log.D("(MongoCli:Query) Running regex based deep search ...")
	qrDeep, err := mc.runSearch(MongoObjsColl, qa, filterMakeRegexSP(qa), retFields)
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

func (mc *Client) GetObjects(ids, retFields []string) (dbms.QueryResults, error) {
	qr, err := mc.runSearch(MongoObjsColl, &dbms.QueryArgs{}, filterMakeIDs(ids), retFields)
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:GetObjects) regex search failed with: %w", err)
	}

	// Success
	return qr, nil
}

func (mc *Client) runSearch(collName string, qa *dbms.QueryArgs,
							spFilter *Filter, retFields []string) (dbms.QueryResults, error) {
	// Create a new filter as a clone of the filter with search phrases
	filter := spFilter.Clone()

	// Is object identifiers specified by query arguments?
	if qa.IsIds() {
		// Need to merge these identifers with search phrases using logical OR
		filter = filterMergeWithIDs(filter, qa.Ids).JoinByOr()
	}

	// Join filter with search phrases and probably identifiers with the
	// query aruments (such mtime, type and so on) using logical AND
	filter = filter.JoinWithOthers(useAnd, filterMakeByArgs(qa))

	// XXX Raw query may be too long
	// log.D("(MongoCli:runSearch) Prepared Mongo filter for search in %q: %v", collName, filter)

	qr, err := mc.aggregateSearch(collName, filter, retFields, qa)
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:runSearch) %w", err)
	}

	return qr, nil
}

func (mc *Client) aggregateSearch(collName string, filter *Filter, retFields []string,
										variadic ...any) (dbms.QueryResults, error) {
	// Make pipline for aggregate operation
	aggrPipeline := mc.makeAggrPipeline(filter, retFields, variadic)

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(collName)

	// Run aggregated query
	cursor, err := coll.Aggregate(mc.Ctx, aggrPipeline)
	if err != nil {
		// Unexpected error
		return nil, fmt.Errorf("(MongoCli:aggregateSearch) aggregate on %s.%s with filter %v failed: %w",
			coll.Database().Name(), coll.Name(), filter, err)
	}
	defer func() {
		if err := cursor.Close(mc.Ctx); err != nil {
			log.E("(MongoCli:aggregateSearch) cannot close cursor: %v", err)
		}
	}()

	// Required fields that must present in the each result item
	rqFields := append([]string{dbms.FieldID}, objMandatoryFields...)

	// Output result
	qr := make(dbms.QueryResults, dbms.ExpectedMaxResults)

	// Make a list of results
	for cursor.Next(mc.Ctx) {
		var item dbms.QRItem
		// Try to decode next cursor value to the item
		if err := cursor.Decode(&item); err != nil {
			log.E("(MongoCli:aggregateSearch) cannot decode cursor item: %v", err)
			// Break cursor loop
			break
		}

		// Check for all required fields are present
		for _, field := range rqFields {
			v, ok := item[field]
			if !ok {
				log.E("(MongoCli:aggregateSearch) Skip returned result without" +
					" required field %q data: %#v", field, item)
				goto nextResult
			}
			// Check that value is string
			if _, ok := v.(string); !ok {
				log.E("(MongoCli:aggregateSearch) Skip returned result with non-string value of" +
					" required field %q, value: %#v (%T)", field, v, v)
				goto nextResult
			}
		}

		// Save result
		qr[types.ObjKey{
			Host: item[dbms.FieldHost].(string),
			Path: item[dbms.FieldFPath].(string)},
		] = item

		// Point to jump if something wrong with result
		nextResult:
	}

	return qr, nil
}

func (mc *Client) makeAggrPipeline(filter *Filter, retFields []string, variadic []any) mongo.Pipeline {
	// Filter-replace pipeline
	aggrPipeline := mongo.Pipeline{
		bson.D{{ `$match`, filter.Expr() }},	// apply filter
	}

	// Create list of requested fields
	fields := bson.D{}
	for _, field := range retFields {
		fields = append(fields, bson.E{Key: field, Value: 1})
	}

	// Is requested fields not empty?
	if len(fields) != 0 {
		// Check for some field required to creation of object key was not added
		if kfSet := tools.NewSet(objMandatoryFields...).Del(retFields...); !kfSet.Empty() {
			// Add these fields to request
			for _, field := range kfSet.Sorted() {
				fields = append(fields, bson.E{Key: field, Value: 1})
			}
		}

		// Add $project stage to pipeline to set the requested fields set
		aggrPipeline = append(aggrPipeline, bson.D{{ `$project`, fields }})
	}

	// Add $addFields stage to replace field name _id by id
	aggrPipeline = append(aggrPipeline, bson.D{{`$addFields`, bson.D{
		{MongoFieldID, `$REMOVE`},
		{dbms.FieldID, `$` + MongoFieldID},
	}}})

	// Process variadic arguments and return created pipeline
	return pipelineConfVariadic(filter, aggrPipeline, variadic)
}

func (mc *Client) delFieldByID(collName, field string, ids []string) (int64, error) {
	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(collName)

	res, err := coll.UpdateMany(mc.Ctx,
		filterMakeIDs(ids).Expr(),					// set filter
		bson.D{{`$unset`, bson.D{{field, nil}}}})	// unset field value
	if err != nil {
			return 0, fmt.Errorf("(MongoCli:delFieldByID) cannot remove field %q from %q: %w", field, ids, err)
	}

	if res.MatchedCount == 0 && res.ModifiedCount == 0 {
		return 0, fmt.Errorf("(MongoCli:delFieldByID) updateMany (ids: %v) on %s.%s returned success," +
			" but no documents were changed", ids, coll.Database().Name(), coll.Name())
	}

	return res.ModifiedCount, nil
}
