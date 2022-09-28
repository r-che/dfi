package main

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func doDel(dbc dbi.DBClient) error {
	// Get configuration
	c := cfg.Config()

	var err error
	switch {
		case c.UseTags:
			err = delTags(dbc, c.CmdArgs[0], c.CmdArgs[1:])
		case c.UseDescr:
			err = delDescr(dbc, c.CmdArgs)
		default:
			panic("unexpected set mode")
	}

	if err == nil {
		fmt.Println("OK")
	}

	// OK
	return err
}

func delTags(dbc dbi.DBClient, tagsStr string, ids []string) error {
	// Get configuration
	c := cfg.Config()

	// Split tags string and remove empty lines if exists
	tags := strings.Split(tagsStr, ",")
	for i := 0; i < len(tags); {
		// Remove leading/trailing spaces from tag
		tags[i] = strings.TrimSpace(tags[i])

		// Check tag for special value forbidden to set
		if tags[i] == dbi.AIIAllTags {
			// Need to clear all tags, skip processing other tags
			tags = []string{dbi.AIIAllTags}
			break
		}

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
	if err := dbc.ModifyAII(dbi.Delete, args, ids, c.SetAdd); err != nil {
		return fmt.Errorf("cannot delete tags: %v", err)
	}

	// OK
	return nil
}

func delDescr(dbc dbi.DBClient, ids []string) error {
	return fmt.Errorf("DEL DESCR Not implemented")
}
