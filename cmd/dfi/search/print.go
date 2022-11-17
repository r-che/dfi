package search

import (
	"fmt"
	"sort"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/cmd/dfi/internal/cfg"
)

func printDupes(refObjs map[string]*types.FSObject, dm map[string][]dupeInfo) {
	// Get configuration
	c := cfg.Config()
	// Get list of requested identifiers to print results in the same order
	ids := c.CmdArgs

	// Remove identifiers for which do not have duplicates/were removed as unsuitable to search
	for i := 0; i < len(ids); {
		_, ok := refObjs[ids[i]]
		if !ok {
			// Remove from identifiers list
			ids = append(ids[:i], ids[i+1:]...)
			continue
		}

		i++
	}

	// Print results
	switch {
	// JSON output
	case c.JSONOut:
		printJSONDupes(ids, dm)

	// Single-lined output
	case c.OneLine:
		printDupesOL(ids, dm)

	// Normal verbose multiline output
	default:
		printDupesVerb(ids, refObjs, dm)
	}
}

func printDupesOL(ids []string, dm map[string][]dupeInfo) {
	for _, id := range ids {
		// Get duplicates for id
		dupes := dm[id]
		// Sort by object keys
		sort.Slice(dupes, func(i, j int) bool {
			return dupes[i].objKey.Less(dupes[j].objKey)
		})

		// Print reference identifier
		fmt.Print(id)
		// Print all dupes of id
		for _, did := range dupes {
			fmt.Printf(" %s", did.id)
		}
		// Print new line
		fmt.Println()
	}
}

func printDupesVerb(ids []string, refObjs map[string]*types.FSObject, dm map[string][]dupeInfo) {
	for i, id := range ids {
		// Get duplicates for this object
		dupes := dm[id]
		// Sort duplicates by object keys
		sort.Slice(dupes, func(i, j int) bool {
			return dupes[i].objKey.Less(dupes[j].objKey)
		})

		// Get key of reference object
		objKey := refObjs[id].FPath	// XXX loadDupesRefs() kept object key in this field

		// Is no duplicates were found
		if len(dupes) == 0 {
			fmt.Printf("%s %s: No duplicates were found\n", id, objKey)
		} else {
			// Print all dupes of reference object
			fmt.Printf("%s %s (%d):\n", id, objKey, len(dupes))
			for _, did := range dupes {
				fmt.Printf("  %s\n", did)
			}
		}

		if i != len(ids) - 1 {
			fmt.Print("---")
		}
		fmt.Println()
	}
}

func printJSONDupes(ids []string, dm map[string][]dupeInfo) {
	// Get configuration
	c := cfg.Config()

	if len(ids) == 0 {
		fmt.Println(`{}`)
		return
	}

	// New line if required
	nl := "\n"
	// Indent if required
	ind := "    "
	if c.OneLine {
		// Clear new line character and indentation
		nl = ""
		ind = ""
	}

	// Start of JSON container
	fmt.Print(`{` + nl)

	// Print items
	for i, id := range ids {
		// Get duplicates for this object
		dupes := dm[id]
		// Sort duplicates by object keys
		sort.Slice(dupes, func(i, j int) bool {
			return dupes[i].objKey.Less(dupes[j].objKey)
		})

		// Is no duplicates were found
		if len(dupes) == 0 {
			// No duplicates found
			fmt.Printf(ind + `%q: {}`, id)
		} else {
			fmt.Printf(ind + `%q: {` + nl, id)
			// Print all dupes of reference object
			for j, dinf := range dupes {
				fmt.Printf(ind + ind + `%q: %q`, dinf.id, dinf.objKey)
				if j != len(dupes) - 1 {
					fmt.Print(`,`)
				}
				fmt.Print(nl)
			}
			fmt.Print(ind + `}`)
		}

		if i != len(ids) - 1 {
			fmt.Print(`,`)
		}
		fmt.Print(nl)
	}

	// End of JSON container
	fmt.Print(`}` + "\n")
}
