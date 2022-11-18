package tools

import (
	"fmt"
	"strings"

	"golang.org/x/exp/constraints"
)

//
// Set - set of items
//
type Set[T constraints.Ordered] map[T]bool

func NewSet[T constraints.Ordered](values ...T) Set[T] {
	ss := make(Set[T], len(values))
	for _, v := range values {
		ss[v] = true
	}

	return ss
}

func (ss Set[T]) Add(values ...T) Set[T] {
	for _, v := range values {
		ss[v] = true
	}

	return ss
}

func (ss Set[T]) Del(values ...T) Set[T] {
	for _, v := range values {
		delete(ss, v)
	}

	return ss
}

func (ss Set[T]) Sorted() []T {
	// Unique items list
	sorted := make([]T, 0, len(ss))
	for s := range ss {
		sorted = append(sorted, s)
	}

	return SortUniqItems(sorted)
}

func (ss Set[T]) List() []T {
	// Unique items list
	items := make([]T, 0, len(ss))
	for s := range ss {
		items = append(items, s)
	}

	return items
}

func (ss Set[T]) Includes(v T) bool {
	_, ok := ss[v]
	return ok
}

func (ss Set[T]) Complement(values ...T) []T {
	compl := make([]T, 0, len(values))
	for _, v := range values {
		if _, ok := ss[v]; !ok {
			compl = append(compl, v)
		}
	}

	return SortUniqItems(compl)
}

func (ss Set[T]) AddComplement(values ...T) []T {
	compl := make([]T, 0, len(values))
	for _, v := range values {
		// Is value already exists?
		if _, ok := ss[v]; ok {
			// Skip it
			continue
		}

		// Add value to the complement
		compl = append(compl, v)

		// Add to the set
		ss[v] = true
	}

	return SortUniqItems(compl)
}

func (ss Set[T]) String() string {
	out := strings.Builder{}

	items := ss.Sorted()

	out.WriteString("(")
	for i, v := range items {
		out.WriteString(fmt.Sprintf("%v", v))
		if i != len(items)-1 {
			out.WriteString(", ")
		}
	}
	out.WriteString(")")

	return out.String()
}

func (ss Set[T]) Empty() bool {
	return len(ss) == 0
}

func (ss Set[T]) Len() int {
	return len(ss)
}
