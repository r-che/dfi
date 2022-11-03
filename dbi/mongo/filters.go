package mongo

import (
	"fmt"
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

type queryJoinType byte
const (
	useAnd	=	queryJoinType(iota)
	useOr
)

type Filter struct {
	expr primitive.D	// search expression
	// TODO Some flags
}

func NewFilter() *Filter {
	return &Filter{
		// TODO expression: primitive.D{},
	}
}

func (f *Filter) Clone() *Filter {
	// Return new filter with the same expression
	return NewFilter().SetExpr(f.expr...)
}

func (f *Filter) SetExpr(expr ...primitive.E) *Filter {
	// Allocate new expression slice
	f.expr = make(primitive.D, len(expr))
	// Copy expression
	copy(f.expr, expr)

	return f
}

func (f *Filter) Append(expr ...primitive.E) *Filter {
	for _, item := range expr {
		f.expr = append(f.expr, item)
	}

	return f
}

func (f *Filter) JoinByNor() *Filter {
	if len(f.expr) == 0 {
		// Nothing to join, just return clone
		return NewFilter().Clone()
	}

	if len(f.expr) == 1 {
		// Return logical negation of condition
		return f.NegFilter()
	}

	norConds := make([]bson.D, 0, len(f.expr))
	for _, cond := range f.expr {
		norConds = append(norConds, bson.D{cond})
	}

	return NewFilter().SetExpr(bson.E{`$nor`, norConds})
}

func (f *Filter) JoinByOr() *Filter {
	if len(f.expr) < 2 {
		// Nothing to join, return clone
		return f.Clone()
	}

	orConds := make([]bson.D, 0, len(f.expr))
	for _, cond := range f.expr {
		orConds = append(orConds, bson.D{cond})
	}

	return f.Clone().
		SetExpr(bson.E{`$or`, orConds})
}

// NegFilter converts list of filter conditions from:
// { { $cond1 }, { $cond2 }, { $cond3 } }
// to:
// {$not: $cond1 }, {$not: $cond2 }, {$not: $cond3}
// but (!) Mongo does not support $not as the first-level operator, need to use $nor
// { {$nor: [{ $cond1 }] }, {$nor: [{ $cond2 }], {$nor: [{ $cond3 }]} }
func (f *Filter) NegFilter() *Filter {
	neg := bson.D{}
	for _, cond := range f.expr {
		neg = append(neg, bson.E{
			Key:	`$nor`,
			Value:	primitive.A{bson.D{cond}},
		})
	}

	return NewFilter().SetExpr(neg...)
}

// XXX --- New functions ----

// makeFilterByArgs makes filter expression to use search arguments like mtime, type of object and so on
func makeFilterByArgs_New(qa *dbms.QueryArgs) *Filter {
	//
	// Build search arguments filter
	//
	filter := NewFilter()

	if qa.IsMtime() {
		filter.Append(makeSetRangeQuery(dbms.FieldMTime, qa.MtimeStart, qa.MtimeEnd, qa.MtimeSet))
	}

	if qa.IsSize() {
		filter.Append(makeSetRangeQuery(dbms.FieldSize, qa.SizeStart, qa.SizeEnd, qa.SizeSet))
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

// makeFilterRegexSP makes filter to search by fpath and rpath fields using regular expression
func makeFilterRegexSP_New(qa *dbms.QueryArgs) *Filter {
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

// makeFilterIDs makes filter to search documents by identifiers specified by ids
func makeFilterIDs_New(ids []string) *Filter {
	return NewFilter().SetExpr(bson.E{
		MongoIDField, bson.D{bson.E{`$in`, ids},
	}})
}

// makeFilterFullTextSearch makes filter to use full-text search by search phrases
func makeFilterFullTextSearch_New(qa *dbms.QueryArgs) *Filter {
	// Check for any search phrases set
	if len(qa.SP) == 0 {
		// No search phrases
		return nil
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

	return NewFilter().SetExpr(bson.E{`$text`,
		bson.D{{ `$search`, strings.Join(prepared, " ")},
	}})
}

// mergeIdsWithSPs merges filters by identifiers to existing filter with search expression with search phrases
func mergeIdsWithSPs_New(qa *dbms.QueryArgs, sp bson.D) *Filter {
	filter := NewFilter().SetExpr(sp...)
	// Append expressions to search by identifiers
	for _, id := range qa.Ids {
		filter.Append(bson.E{MongoIDField, id})
	}

	return filter
}

func joinFilters_New(qjt queryJoinType, conds ...bson.D) *Filter {
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
		return NewFilter()
	}

	if len(conds) == 1 {
		// Return new filter created from the first condition
		return NewFilter().SetExpr(conds[0]...)
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

	return NewFilter().SetExpr(bson.E{ op, conds })
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

// XXX --- OLD FUNCTIONS ---

// makeFilterRegexSP makes filter to search by fpath and rpath fields using regular expression
func makeFilterRegexSP(qa *dbms.QueryArgs) bson.D {
	spFilter := bson.D{}

	if len(qa.SP) != 0 {
		if qa.OnlyName {
			// Use only the "name" field to search
			for _, phrase := range qa.SP {
				spFilter = append(spFilter,
					bson.E{dbms.FieldName, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
				)
			}
		} else {
			//// Use the found path and real path fields to search
			for _, phrase := range qa.SP {
				spFilter = append(spFilter,
					bson.E{dbms.FieldFPath, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
					bson.E{dbms.FieldRPath, primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}},
				)
			}
		}

	}

	return spFilter
}

// makeFilterIDs makes filter to search documents by identifiers specified by ids
func makeFilterIDs(ids []string) bson.D {
	return bson.D{{
		MongoIDField, bson.D{bson.E{`$in`, ids}},
	}}
}

// makeFilterFullTextSearch makes filter to use full-text search by search phrases
func makeFilterFullTextSearch(qa *dbms.QueryArgs) bson.D {
	// Check for any search phrases set
	if len(qa.SP) == 0 {
		// No search phrases
		return nil
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

	return bson.D{{`$text`,
		bson.D{{ `$search`, strings.Join(prepared, " ")}},
	}}
}

// mergeIdsWithSPs merges filters by identifiers to existing filter with search expression with search phrases
func mergeIdsWithSPs(qa *dbms.QueryArgs, sp bson.D) bson.D {
	// Append expressions to search by identifiers
	for _, id := range qa.Ids {
		sp = append(sp, bson.E{MongoIDField, id})
	}

	return sp
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

// makeFilterByArgs makes filter expression to use search arguments like mtime, type of object and so on
func makeFilterByArgs(qa *dbms.QueryArgs) bson.D {
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

	return args
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
