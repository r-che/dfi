package mongo

import (
	"fmt"
	"regexp"

//	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

	//"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	//"go.mongodb.org/mongo-driver/mongo/options"
)

type queryJoinType byte
const (
	useAnd	=	queryJoinType(iota)
	useOr
)

func makeFilter(qa *dbms.QueryArgs) bson.D {
	//
	// Using search phrases
	//

	spAndIds := bson.D{}

	if len(qa.SP) != 0 {
		if qa.OnlyName {
			// Use only the "name" field to search
			for _, phrase := range qa.SP {
				spAndIds = append(spAndIds,
					bson.E{dbms.FieldName, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
				)
			}
		} else {
			//// Use the found path and real path fields to search
			for _, phrase := range qa.SP {
				spAndIds = append(spAndIds,
					bson.E{dbms.FieldFPath, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
					bson.E{dbms.FieldRPath, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
				)
			}
		}

	}

	// Use objects identifiers
	if qa.IsIds() {
		for _, id := range qa.Ids {
			spAndIds = append(spAndIds, bson.E{MongoIDField, id})
		}
	}

	// Join all collected conditions by logical OR (if any)
	spAndIds = joinByOr(spAndIds)


	//
	// Build search arguments filter
	//
	args := bson.D{}

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
		args = append(args, bson.E{dbms.FieldType, bson.D{bson.E{`$in`, qa.Types}}})
	}

	if qa.IsChecksum() {
		//chunks = append(chunks, `@` + dbms.FieldChecksum + `:{` +  strings.Join(qa.CSums, `|`) + `}`)
		args = append(args, bson.E{dbms.FieldChecksum, bson.D{bson.E{`$in`, qa.CSums}}})
	}

	if qa.IsHost() {
		args = append(args, bson.E{dbms.FieldHost, bson.D{ bson.E{`$in`, qa.Hosts}}})
	}

	/*
	// Need to build request from chunks
	// TODO
	if qa.NegExpr {
		negMark = "-"
	}
	*/

	if qa.OrExpr {
		args = joinByOr(args)
	}

	//
	// Construct resulting query
	//

	return joinFilters(useAnd, spAndIds, joinFilters(useAnd, args))
}

func joinByOr(conds bson.D) bson.D {
	if len(conds) < 2 {
		return conds
	}

	orConds := make([]bson.D, 0, len(conds))
	for _, cond := range conds {
		orConds = append(orConds, bson.D{cond})
	}

	return bson.D{
		bson.E{`$or`, orConds},
	}
}

func joinFilters(qjt queryJoinType, conds ...bson.D) bson.D {
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
		panic(fmt.Sprintf(`Unsupported query join type "%d", conditions: %v`, qjt, conds))
	}

	return bson.D{{ op, conds }}
}

func makeSetRangeQuery(field string, min, max int64, set []int64) bson.D {
	return bson.D{} // TODO
	/*
	// Is set is not provided
	if len(set) == 0 {
		// Then min/max query

		// If closed interval
		if min != 0 && max != 0 {
			return fmt.Sprintf(`@%s:[%d %d]`, field, min, max)
		}

		// Half-open interval
		if min == 0 {
			return fmt.Sprintf(`@%s:[-inf %d]`, field, max)
		}

		return fmt.Sprintf(`@%s:[%d +inf]`, field, min)
	}

	// Build query from set of values
	chunks := make([]string, 0, len(set))

	for _, item := range set {
		chunks = append(chunks, fmt.Sprintf(`@%s:[%d %d]`, field, item, item))
	}

	return `(` + strings.Join(chunks, `|`) + `)`
	*/
}
