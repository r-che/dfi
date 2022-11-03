package mongo

import (
	"regexp"
	"strings"

//	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

	//"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	//"go.mongodb.org/mongo-driver/mongo/options"
)

// filterMakeByArgs makes filter expression to use search arguments like mtime, type of object and so on
func filterMakeByArgs(qa *dbms.QueryArgs) *Filter {
	//
	// Build search arguments filter
	//
	filter := NewFilter()

	if qa.IsMtime() {
		filter.Append(filterMakeSetRangeExpr(dbms.FieldMTime, qa.MtimeStart, qa.MtimeEnd, qa.MtimeSet))
	}

	if qa.IsSize() {
		filter.Append(filterMakeSetRangeExpr(dbms.FieldSize, qa.SizeStart, qa.SizeEnd, qa.SizeSet))
	}

	if qa.IsType() {
		filter.Append(bson.E{dbms.FieldType, bson.D{bson.E{`$in`, qa.Types}}})
	}

	if qa.IsChecksum() {
		filter.Append(bson.E{dbms.FieldChecksum, bson.D{bson.E{`$in`, qa.CSums}}})
	}

	if qa.IsHost() {
		filter.Append(bson.E{dbms.FieldHost, bson.D{ bson.E{`$in`, qa.Hosts}}})
	}

	//
	// XXX Processing logical flags --not (NOT) and --or (OR)
	//

	/*
	 * Here wa have a set of conditions in the filter filter list, they looks like:
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
		return filter.JoinByNor()

	// Case #2
	case qa.OrExpr:
		// Rejoing arguments by OR
		return filter.JoinByOr()

	// Case #3
	case qa.NegExpr:
		// 1. Do negation of each condition in filter
		// 2. Join condiions by OR instead of AND
		return filter.JoinByOr().NegFilter()

	// Case #4 is the default configuration - nothing to modify
	default:
		return filter
	}
}

// filterMakeRegexSP makes filter to search by fpath and rpath fields using regular expression
func filterMakeRegexSP(qa *dbms.QueryArgs) *Filter {
	spFilter := NewFilter()

	if len(qa.SP) != 0 {
		if qa.OnlyName {
			// Use only the "name" field to search
			for _, phrase := range qa.SP {
				spFilter.Append(bson.E{
					dbms.FieldName, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}})
			}
		} else {
			//// Use the found path and real path fields to search
			for _, phrase := range qa.SP {
				spFilter.Append(
					bson.E{dbms.FieldFPath, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
					bson.E{dbms.FieldRPath, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
				)
			}
		}

	}

	return spFilter
}

// filterMakeIDs makes filter to search documents by identifiers specified by ids
func filterMakeIDs(ids []string) *Filter {
	return NewFilter().SetExpr(bson.D{bson.E{
		MongoIDField, bson.D{bson.E{`$in`, ids},
	}}})
}

// filterMakeFullTextSearch makes filter to use full-text search by search phrases
func filterMakeFullTextSearch(qa *dbms.QueryArgs) *Filter {
	// Check for any search phrases set
	if len(qa.SP) == 0 {
		// No search phrases, return empty filter
		return NewFilter()
	}

	// Prepare search phrases before constructing query: if the phrase contains whitespaces,
	// it should be interpreted as is - need to enclose it by escaped double quotes[1]
	// [1] https://www.mongodb.com/docs/manual/reference/operator/query/text/#-search-field
	prepared := make([]string, 0, len(qa.SP))
	for _, sp := range qa.SP {
		// Escape backslash with double quote
		sp = strings.ReplaceAll(sp, `\"`, `"`)
		if strings.IndexAny(sp, " \t\n") != -1 {
			// sp contains whitespaces, need to wrap
			sp = `\"` + sp + `\"`
		}

		// Append to prepared
		prepared = append(prepared, sp)
	}

	return NewFilter().
		SetFullText().
		SetExpr(bson.D{bson.E{`$text`,
			bson.D{{ `$search`, strings.Join(prepared, " ")},
		}}})
}

// mergeIdsWithSPs merges filters by identifiers to existing filter with search expression with search phrases
func filterMergeWithIDs(filter *Filter, ids []string) *Filter {
	newFilter := filter.Clone()
	// Append expressions to search by identifiers
	for _, id := range ids {
		newFilter.Append(bson.E{MongoIDField, id})
	}

	return filter
}

func filterMakeSetRangeExpr(field string, min, max int64, set []int64) bson.E {
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
