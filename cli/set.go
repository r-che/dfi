package main

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func doSet(dbc dbi.DBClient) error {
	// Get configuration
	c := cfg.Config()

	args := c.CmdArgs
	setValue := args[0]
	setIDs := args[1:]

	switch {
		case c.UseTags:
			return setTags(dbc, setValue, setIDs, c.SetAdd)
		case c.UseDescr:
			// TODO
			return nil
		default:
			panic("unexpected set mode")
	}

	// OK (unreachable)
	return nil
}

func setTags(dbc dbi.DBClient, tagsStr string, ids []string, add bool) error {
	// Split tags string and remove empty lines if exists
	tags := strings.Split(tagsStr, ",")
	for i := 0; i < len(tags); {
		if tags[i] == "" {
			tags = append(tags[:i], tags[i+1:]...)
		} else {
			i++
		}
	}
	if len(tags) == 0 {
		return fmt.Errorf("invalid tags value from command line: %q", tagsStr)
	}

	args := &dbi.AIIArgs{Tags: tags}
	if err := dbc.ModifyAII(dbi.Update, add, ids, args); err != nil {
		return fmt.Errorf("cannot set tags: %v", err)
	}

	// OK
	return nil
}
