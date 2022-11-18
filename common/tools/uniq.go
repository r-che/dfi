package tools

import (
	"sort"

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
