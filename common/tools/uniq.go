package tools

import (
	"sort"
	"strings"
)

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
func (ss *StrSet) Add(values ...string) {
	for _, v := range values {
		(*ss)[v] = true
	}
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
