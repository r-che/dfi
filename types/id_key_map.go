// Map with a correspondence between the object key and its ID
package types

import (
	"sort"
	"strings"
)

type IDKeyMap map[string]ObjKey

func (ikm IDKeyMap) String() string {
	ids := make([]string, 0, len(ikm))
	for id := range ikm {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return "[" + strings.Join(ids, " ") + "]"
}

func (ikm IDKeyMap) Keys() []string {
	ids := make([]string, 0, len(ikm))
	for id := range ikm {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return ids
}

func (ikm IDKeyMap) KeysAny() []any {
	ids := make([]any, 0, len(ikm))
	for id := range ikm {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].(string) < ids[j].(string)
	})
	return ids
}
