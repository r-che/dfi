package show

import (
	"fmt"
	"sort"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cmd/dfi/internal/cfg"

)

func showTags(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()
	// Get tags list specified from command line
	tags := tools.NewStrSet(c.CmdArgs...)

	// Function's return value
	rv := types.NewCmdRV()

	// Get list of objects with tags
	ids, err := dbc.GetAIIIds([]string{dbms.AIIFieldTags})
	if err != nil {
		return rv.AddErr("cannot get list of objects with tags: %w", err)
	}

	// Get tags by identifiers
	qr, err := dbc.GetAIIs(ids, []string{dbms.AIIFieldTags})
	if err != nil {
		return rv.AddErr("cannot get objects with tags: %w", err)
	}

	// Create tags-usage map - key is a tag, value - how many times the tag is used
	tt := tagsUsageMap(qr, tags, c.Quiet)

	//
	// Make a list tags sorted by number of times used
	//
	keys := make([]string, 0, len(tt))
	for k := range tt {
		keys = append(keys, k)
	}

	// Sort in REVERSE order - the higher values should be the first
	sort.Slice(keys, func(i, j int) bool {
		// Compare by number of occurrences
		if tt[keys[i]] > tt[keys[j]] {
			return true
		}
		if tt[keys[i]] == tt[keys[j]] {
			// Need to compare by keys in alphabetical order
			return keys[i] < keys[j]
		}

		return false
	})

	// Produce output
	showTagsOutput(keys, tt, c.Quiet)

	return rv.AddFound(int64(len(tt)))
}

func tagsUsageMap(qr dbms.QueryResultsAII, tags *tools.StrSet, quiet bool) map[string]int {
	// Map with tag<=>times
	tt := map[string]int{}

	// Check that tags were not provided by command line
	if tags.Empty() {
		// Add all tags
		for _, aii := range qr {
			for _, tag := range aii.Tags {
				tt[tag]++
			}
		}

		// Return resulted map
		return tt
	}

	// Add only specified tags
	for _, aii := range qr {
		for _, tag := range aii.Tags {
			if (*tags)[tag] {
				tt[tag]++
			}
		}
	}
	// If not quiet mode - need to add any not found but requested tags with zero values
	if !quiet {
		for _, tag := range tags.List() {
			if _, ok := tt[tag]; !ok {
				tt[tag] = 0
			}
		}
	}

	return tt
}


func showTagsOutput(keys []string, tt map[string]int, quiet bool) {
	// Produce output
	if quiet {
		// Quiet mode - only tags with non-zero number of occurrences
		for _, k := range keys {
			fmt.Println(k)
		}
	} else {
		// Normal mode - print: number_occurrences\ttag\n
		for _, k := range keys {
			fmt.Printf("%d\t%s\n", tt[k], k)
		}
	}
}
