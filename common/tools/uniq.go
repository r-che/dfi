package tools

import (
	"sort"
	"strings"

	"golang.org/x/exp/constraints"
)

func SortUniqItems[T constraints.Ordered](items []T) []T {
	// Make items unique
	nextUniq := 0
	uniqs := make(map[T]bool, len(items))

	for _, item := range items {
		// Check for item already present
		if uniqs[item] {
			// Skip it
			continue
		}

		// Add item to uniqs
		uniqs[item] = true

		// Move unique item to the next position of unique item
		items[nextUniq] = item

		// Increase next unique position
		nextUniq++
	}

	// Update source slice value by "uniquefied" list
	items = items[:nextUniq]

	// Now, sort it
	sort.Slice(items, func(i, j int) bool {
		return items[i] < items[j]
	})

	// Return sorted list of unique elements
	return items
}

//
// StrSet - set of strings
//
type StrSet map[string]bool
func NewStrSet(values ...string) *StrSet {
	ss := make(StrSet, len(values))
	for _, v := range values {
		ss[v] = true
	}

	return &ss
}
func (ss *StrSet) Add(values ...string) *StrSet {
	for _, v := range values {
		(*ss)[v] = true
	}

	return ss
}
func (ss *StrSet) Includes(v string) bool {
	_, ok := (*ss)[v]
	return ok
}
func (ss *StrSet) Del(values ...string) *StrSet {
	for _, v := range values {
		delete(*ss, v)
	}

	return ss
}
func (ss *StrSet) Complement(values ...string) []string {
	compl := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := (*ss)[v]; !ok {
			compl = append(compl, v)
		}
	}

	return NewStrSet(compl...).List()	// make values unique
}
func (ss *StrSet) AddComplement(values ...string) []string {
	compl := make([]string, 0, len(values))
	for _, v := range values {
		// Is value already exists?
		if _, ok := (*ss)[v]; ok {
			// Skip it
			continue
		}

		// Add value to the complement
		compl = append(compl, v)

		// Add to the set
		(*ss)[v] = true
	}

	return NewStrSet(compl...).List()	// make values unique
}
func (ss *StrSet) List() []string {
	// Unique strings list
	uStr := make([]string, 0, len(*ss))
	for s := range *ss {
		uStr = append(uStr, s)
	}
	// Sort
	sort.Strings(uStr)

	return uStr
}
func (ss *StrSet) String() string {
	return "(" + strings.Join(ss.List(), ", ") + ")"
}
func (ss *StrSet) Empty() bool {
	return len(*ss) == 0
}
