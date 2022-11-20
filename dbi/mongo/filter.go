package mongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type queryJoinType byte
const (
	useAnd	=	queryJoinType(iota)
	useOr
)

type Filter struct {
	expr bson.D	// search expression

	// Filter flags
	ftSearch	bool	// MUST be true if filter contains $text operator
}

func NewFilter() *Filter {
	return &Filter{}
}

func (f *Filter) Expr() bson.D {
	return f.expr
}

func (f *Filter) Len() int {
	return len(f.expr)
}

func (f *Filter) Clone() *Filter {
	// Make a copy
	rv := *f

	// Make a copy of expression slice using SetExpr
	rv.SetExpr(f.expr)

	// Return the pointer to the copy
	return &rv
}

func (f *Filter) SetExpr(expr primitive.D) *Filter {
	if expr == nil {
		return f
	}

	// Allocate new expression slice
	f.expr = make(primitive.D, len(expr))
	// Copy expression
	copy(f.expr, expr)

	return f
}

func (f *Filter) SetFullText() *Filter {
	f.ftSearch = true

	return f
}

func (f *Filter) FullText() bool {
	return f.ftSearch
}

func (f *Filter) Append(expr ...primitive.E) *Filter {
	for _, item := range expr {
		f.expr = append(f.expr, item)
	}

	return f
}

func (f *Filter) JoinByNor() *Filter {
	if f.Len() == 0 {
		// Nothing to join, just return clone
		return f.Clone()
	}

	if f.Len() == 1 {
		// Return logical negation of condition
		return f.NegFilter()
	}

	norConds := make([]bson.D, 0, f.Len())
	for _, cond := range f.expr {
		norConds = append(norConds, bson.D{cond})
	}

	return f.Clone().SetExpr(bson.D{bson.E{`$nor`, norConds}})
}

func (f *Filter) JoinByOr() *Filter {
	if f.Len() <= 1 {
		// Cannot join one or zero field, just return clone
		return f.Clone()
	}

	orConds := make([]bson.D, 0, f.Len())
	for _, cond := range f.expr {
		orConds = append(orConds, bson.D{cond})
	}

	return f.Clone().
		SetExpr(bson.D{bson.E{`$or`, orConds}})
}

func (f *Filter) JoinWithOthers(qjt queryJoinType, filters ...*Filter) *Filter {
	joined := []bson.D{}

	if f.Len() != 0 {
		joined = append(joined, f.expr)
	}

	// Add non-empty expressions
	for _, filter := range filters {
		if filter.Len() != 0 {
			joined = append(joined, filter.Expr())
		}
	}

	if len(joined) == 0 {
		// Return empty filter
		return f.Clone()
	}

	if len(joined) == 1 {
		// Return new filter created from the first condition
		return f.Clone().SetExpr(joined[0])
	}

	var op string
	switch qjt {
	case useAnd:
		op = `$and`
	case useOr:
		op = `$or`
	default:
		panic(fmt.Sprintf(`Unsupported query join type "%d", filters: %v`, qjt, joined))
	}

	return f.Clone().SetExpr(bson.D{bson.E{ op, joined }})
}

// NegFilter converts list of filter conditions from:
//   { { $cond1 }, { $cond2 }, { $cond3 } }
// to:
//   {$not: $cond1 }, {$not: $cond2 }, {$not: $cond3}
// but (!) Mongo does not support $not as the first-level operator, need to use $nor
//   { {$nor: [{ $cond1 }] }, {$nor: [{ $cond2 }], {$nor: [{ $cond3 }]} }
func (f *Filter) NegFilter() *Filter {
	neg := bson.D{}
	for _, cond := range f.expr {
		neg = append(neg, bson.E{
			Key:	`$nor`,
			Value:	primitive.A{bson.D{cond}},
		})
	}

	return f.Clone().SetExpr(neg)
}
