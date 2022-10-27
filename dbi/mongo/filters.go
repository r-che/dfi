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

	if qa.IsMtime() {
		args = append(args, makeSetRangeQuery(dbms.FieldMTime, qa.MtimeStart, qa.MtimeEnd, qa.MtimeSet))
	}

	if qa.IsSize() {
		args = append(args, makeSetRangeQuery(dbms.FieldSize, qa.SizeStart, qa.SizeEnd, qa.SizeSet))
	}

	if qa.IsType() {
		args = append(args, bson.E{dbms.FieldType, bson.D{bson.E{`$in`, qa.Types}}})
	}

	if qa.IsChecksum() {
		args = append(args, bson.E{dbms.FieldChecksum, bson.D{bson.E{`$in`, qa.CSums}}})
	}

	if qa.IsHost() {
		args = append(args, bson.E{dbms.FieldHost, bson.D{ bson.E{`$in`, qa.Hosts}}})
	}

	//
	// XXX Processing logical flags --not (NOT) and --or (OR)
	//

	/*
	 * Here wa have a set of conditions in the args filter list, they looks like:
	 *  $cond1, $cond2, $cond3, ...
	 *
	 * Need to consider the following cases:
	 *
	 * 1. NOT and OR are set - we have something like:
	 *    > !($cond1 || $cond2 || $cond3 || ...)
	 *    Use De Morgan's laws again, we will get:
	 *    > !$cond1 && !$cond2 && !$cond3 && ...
	 *    That corresponds to the NOR operator, which supported by Mongo natively:
	 *    > $nor: [ $cond1, $cond2 ]
	 *
	 * 2. OR is set - need to rejoin list of condition by logical OR,
	 *    that can be done using $or Mongo operator:
	 *    > $or: [ $cond1, $cond2, $cond3, ... ]
	 *
	 * 3. NOT is set - we have something like:
	 *    > !($cond1 && $cond2 && $cond3)
	 *    Because of Mongo does not support $not as the first level operation, we need
	 *    to use De Morgan's laws to convert expression to:
	 *    > !$cond1 || !$cond2 || !$cond3 || ....
	 *    In Mongo-like notation:
	 *    > $or: [ {$not: $cond1}, {$not: $cond2}, {$not: $cond3} ]
	 *
	 * 4. Neither of logical flags is set: no additional transformations required, by default
	 *    all conditions within the filter document are treated as joined by logical AND:
	 *    > $cond1 && $cond2 && $cond3 && ...
	*/

	switch {
	// Case #1
	case qa.NegExpr && qa.OrExpr:
		args = joinByNor(args)

	// Case #2
	case qa.OrExpr:
		// Rejoing arguments by OR
		args = joinByOr(args)

	// Case #3
	case qa.NegExpr:
		// 1. Do negation of each condition in args
		// 2. Join condiions by OR instead of AND
		args = joinByOr(negFilter(args))

	// Case #4 is the default configuration - nothing to check
	}

	//
	// Construct resulting query
	//

	return joinFilters(useAnd, spAndIds, joinFilters(useAnd, args))
}

func joinByNor(conds bson.D) bson.D {
	if len(conds) == 0 {
		// Nothing to join, return as is
		return conds
	}
	if len(conds) == 1 {
		// Return logical negation of condition
		return negFilter(conds)
	}

	norConds := make([]bson.D, 0, len(conds))
	for _, cond := range conds {
		norConds = append(norConds, bson.D{cond})
	}

	return bson.D{
		bson.E{`$nor`, norConds},
	}
}

// negFilter converts list of filter conditions from:
// { { $cond1 }, { $cond2 }, { $cond3 } }
// to:
// {$not: $cond1 }, {$not: $cond2 }, {$not: $cond3}
// but (!) Mongo does not support $not as the first-level operator, need to use $nor
// { {$nor: [{ $cond1 }] }, {$nor: [{ $cond2 }], {$nor: [{ $cond3 }]} }
func negFilter(conds bson.D) bson.D {
	neg := bson.D{}
	for _, cond := range conds {
		neg = append(neg, bson.E{
			Key:	`$nor`,
			Value:	primitive.A{bson.D{cond}},
		})
	}
	return neg
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

func makeSetRangeQuery(field string, min, max int64, set []int64) bson.E {
	// Is set is not provided
	if len(set) == 0 {
		//
		// Then range min..max query
		//

		// If closed interval
		if min != 0 && max != 0 {
			return bson.E{field, bson.D{
				{`$gte`, min},	// greater or equal then min
				{`$lte`, max},	// lesser or equal then max
			}}
		}

		// Half-open interval

		if min == 0 {
			// Only the top value of the range specified
			return bson.E{field, bson.D{
				{`$lte`, max},	// lesser or equal then max
			}}
		}

		// Only the bottom value of the range specified
		return bson.E{field, bson.D{
			{`$gte`, min},	// greater or equal then min
		}}
	}

	// Build query from set of values
	return bson.E{field, bson.D{bson.E{`$in`, set}}}
}
