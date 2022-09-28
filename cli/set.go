package main

import (
	"fmt"
	"strings"
	"sort"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"

	"github.com/r-che/log"
)

func doSet(dbc dbi.DBClient) (int64, error) {
	// Get configuration
	c := cfg.Config()

	setValue := c.CmdArgs[0]
	setIDs := c.CmdArgs[1:]

	switch {
		case c.UseTags:
			return setTags(dbc, setValue, setIDs)
		case c.UseDescr:
			return setDescr(dbc, setValue, setIDs)
		default:
			panic("unexpected set mode")
	}

	// Unreachable
	return -1, fmt.Errorf("Unreachable code")
}

func setTags(dbc dbi.DBClient, tagsStr string, ids []string) (int64, error) {
	// Get configuration
	c := cfg.Config()

	log.D("Set tags (append: %t) %q for: %v", c.SetAdd, tagsStr, ids)

	// Split tags string and remove empty lines if exists
	tags := strings.Split(tagsStr, ",")
	for i := 0; i < len(tags); {
		// Remove leading/trailing spaces from tag
		tags[i] = strings.TrimSpace(tags[i])

		// Check tag for special value forbidden to set
		if tags[i] == dbi.AIIAllTags {
			return 0, fmt.Errorf("tag value %q is a special value that cannot be used as a tag", dbi.AIIAllTags)
		}

		// Remove empty tags
		if tags[i] == "" {
			tags = append(tags[:i], tags[i+1:]...)
		} else {
			i++
		}
	}
	if len(tags) == 0 {
		return 0, fmt.Errorf("invalid tags value from command line: %q", tagsStr)
	}

	// Sort list of tags
	sort.Strings(tags)

	updated, _, err := dbc.ModifyAII(dbi.Update, &dbi.AIIArgs{Tags: tags}, ids, c.SetAdd)
	if err != nil {
		return updated, fmt.Errorf("cannot set tags: %v", err)
	}

	// OK
	return updated, nil
}

func setDescr(dbc dbi.DBClient, descr string, ids []string) (int64, error) {
	// Get configuration
	c := cfg.Config()

	log.D("Set description (append: %t) %q for: %v", c.SetAdd, descr, ids)

	// Trim spaces from description and set it to argumets
	args := &dbi.AIIArgs{Descr: strings.TrimSpace(descr), NoNL: c.NoNL}
	_, updated, err := dbc.ModifyAII(dbi.Update, args, ids, c.SetAdd)
	if err != nil {
		return updated, fmt.Errorf("cannot set description: %v", err)
	}

	// OK
	return updated, nil
}
