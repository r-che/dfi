package tools

import "sort"

// UniqStrings returns sorted set of unique values from list
func UniqStrings(list []string) []string {
	// Map to make unique values
	uniq := map[string]any{}
	for _, id := range list {
		uniq[id] = nil
	}

	// Unique strings list
	uStr := make([]string, 0, len(uniq))
	for s := range uniq {
		uStr = append(uStr, s)
	}

	// Sort
	sort.Strings(uStr)

	return uStr
}
