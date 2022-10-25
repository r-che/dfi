package mongo

import (
	"fmt"
	"encoding/json"

//	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	//"go.mongodb.org/mongo-driver/mongo/options"
)

type queryJoinType byte
const (
	useAnd	=	queryJoinType(iota)
	useOr
)

//
// CLI/Web/REST clients interface
//
func printQuery(q any) {
	// TODO Need to use this function instead of %#v in logging
    jsonData, err := json.Marshal(q)
    if err != nil {
        panic(err)
    }
    fmt.Printf("%s\n", jsonData)
}

func (mc *MongoClient) Query(qa *dbms.QueryArgs, retFields []string) (qr dbms.QueryResults, err error) {
	filter := makeFilter(qa)
	printQuery(filter) // TODO

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(ObjsCollection)

	// Run query
	cursor, err := coll.Find( mc.Ctx, filter, /*options.Find().SetReturnKey(true)*/) // TODO Fix options - need to set requested set of fields
	if err != nil {
		// Unexpected error
		return nil, fmt.Errorf("(MongoCli:Query) find on %s.%s with filter %#v failed: %v",
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

		fmt.Printf("Item: %v\n", item)	// TODO
	}

	fmt.Println()
	return nil, fmt.Errorf("Query - Not implemented")	// TODO
}

func makeFilter(qa *dbms.QueryArgs) bson.D {
	//
	// Using search phrases
	//

	spAndIds := bson.D{}
	spAndIdsChunks := bson.A{}

	if len(qa.SP) != 0 {
		if qa.OnlyName {
			// Use only the "name" field to search
			for _, phrase := range qa.SP {
				spAndIdsChunks = append(spAndIdsChunks,
					bson.D{{dbms.FieldName, primitive.Regex{Pattern: phrase, Options: "i"}}},	// TODO Use https://pkg.go.dev/regexp#QuoteMeta to escape
				)
			}
		} else {
			//// Use the found path and real path fields to search
			for _, phrase := range qa.SP {
				spAndIdsChunks = append(spAndIdsChunks,
					bson.D{{dbms.FieldFPath, primitive.Regex{Pattern: phrase, Options: "i"}}},	// TODO Use https://pkg.go.dev/regexp#QuoteMeta to escape
					bson.D{{dbms.FieldRPath, primitive.Regex{Pattern: phrase, Options: "i"}}},	// TODO Use https://pkg.go.dev/regexp#QuoteMeta to escape
				)
			}
		}

	}

	//
	// Using identifiers
	//
	if qa.IsIds() {
	//	chunks = append(chunks, `@` + dbms.FieldID + `:{` +  strings.Join(qa.Ids, `|`) + `}`)
		for _, id := range qa.Ids {
			spAndIdsChunks = append(spAndIdsChunks, bson.D{{dbms.FieldID, id}})
		}
	}

	// Set search phrases condition
	if len(spAndIdsChunks) != 0 {
		spAndIds = bson.D{ { `$or`, spAndIdsChunks } }
	}


	//
	// Using search arguments
	//

	args := []bson.D{}

	/*
	// TODO
	if qa.IsMtime() {
		chunks = append(chunks, makeSetRangeQuery(dbms.FieldMTime, qa.MtimeStart, qa.MtimeEnd, qa.MtimeSet))
	}
	// TODO
	if qa.IsSize() {
		chunks = append(chunks, makeSetRangeQuery(dbms.FieldSize, qa.SizeStart, qa.SizeEnd, qa.SizeSet))
	}
	*/
	if qa.IsType() {
		//chunks = append(chunks, `@` + dbms.FieldType + `:{` +  strings.Join(qa.Types, `|`) + `}`)
		args = append(args, bson.D{{dbms.FieldType, bson.D{bson.E{`$in`, qa.Types}}}})
	}

	if qa.IsChecksum() {
		//chunks = append(chunks, `@` + dbms.FieldChecksum + `:{` +  strings.Join(qa.CSums, `|`) + `}`)
		args = append(args, bson.D{{dbms.FieldChecksum, bson.D{bson.E{`$in`, qa.CSums}}}})
	}

	if qa.IsHost() {
		args = append(args, bson.D{{dbms.FieldHost, bson.D{ bson.E{`$in`, qa.Hosts}}}})
	}

	/*
	// Need to build request from chunks
	// TODO
	if qa.NegExpr {
		negMark = "-"
	}

	// TODO
	if qa.OrExpr {
		argsQuery = fmt.Sprintf(`%s(%s)`, negMark, strings.Join(chunks, ` | `))
	} else {
		argsQuery = fmt.Sprintf(`%s(%s)`, negMark, strings.Join(chunks, ` `))
	}
	*/

	//
	// Construct resulting query
	//


	return joinQuery(useAnd, spAndIds, joinQuery(useAnd, args...))
}

func joinQuery(qjt queryJoinType, conds ...bson.D) bson.D {
	// Remove empty conditions
	for i := 0; i < len(conds); {
		if len(conds[i]) == 0 {
			// Remove condition from slice
			conds = append(conds[:i], conds[i+1:]...)
		}
		i++
	}

	if len(conds) == 0 {
		// Return empty query
		return bson.D{}
	}

	if len(conds) == 1 {
		// Nothing to do, return first element as is
		return conds[0]
	}

	var op string
	switch qjt {
	case useAnd:
		op = `$and`
	case useOr:
		op = `$or`
	default:
		panic(fmt.Sprintf(`Unsupported query join type "%d", conditions: %#v`, qjt, conds))
	}

	return bson.D{{ op, conds }}
}

func makeSetRangeQuery(field string, min, max int64, set []int64) string {
	return "" // TODO
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
