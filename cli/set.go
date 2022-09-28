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

	var err error
	switch {
		case c.UseTags:
			err = setTags(dbc, setValue, setIDs)
		case c.UseDescr:
			err = setDescr(dbc, setValue, setIDs)
		default:
			panic("unexpected set mode")
	}

	if err == nil {
		fmt.Println("OK")
	}

	// OK
	return nil
}

func setTags(dbc dbi.DBClient, tagsStr string, ids []string) error {
	// Get configuration
	c := cfg.Config()

	// Split tags string and remove empty lines if exists
	tags := strings.Split(tagsStr, ",")
	for i := 0; i < len(tags); {
		// Remove leading/trailing spaces from tag
		tags[i] = strings.TrimSpace(tags[i])

		// Remove empty tags
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
	if err := dbc.ModifyAII(dbi.Update, args, ids, c.SetAdd); err != nil {
		return fmt.Errorf("cannot set tags: %v", err)
	}

	// OK
	return nil
}

func setDescr(dbc dbi.DBClient, descr string, ids []string) error {
	// Get configuration
	c := cfg.Config()

	// Trim spaces from description and set it to argumets
	args := &dbi.AIIArgs{Descr: strings.TrimSpace(descr), NoNL: c.NoNL}
	if err := dbc.ModifyAII(dbi.Update, args, ids, c.SetAdd); err != nil {
		return fmt.Errorf("cannot set description: %v", err)
	}

	// OK
	return nil
}
