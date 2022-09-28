package main

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"

	"github.com/r-che/log"
)

func doDel(dbc dbi.DBClient) (int64, error) {
	// Get configuration
	c := cfg.Config()

	var err error
	var changed int64

	switch {
		case c.UseTags:
			changed, err = delTags(dbc, c.CmdArgs[0], c.CmdArgs[1:])
		case c.UseDescr:
			err = delDescr(dbc, c.CmdArgs)
		default:
			panic("unexpected set mode")
	}

	return changed, err
}

func delTags(dbc dbi.DBClient, tagsStr string, ids []string) (int64, error) {
	// Get configuration
	c := cfg.Config()

	log.D("Delete tags %q from: %v", tagsStr, ids)

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
		return 0, fmt.Errorf("invalid tags value from command line: %q", tagsStr)
	}

	args := &dbi.AIIArgs{Tags: tags}
	changed, _, err := dbc.ModifyAII(dbi.Delete, args, ids, c.SetAdd)
	if err != nil {
		return changed, fmt.Errorf("cannot delete tags: %v", err)
	}

	// OK
	return changed, nil
}

func delDescr(dbc dbi.DBClient, ids []string) error {
	return fmt.Errorf("DEL DESCR Not implemented")	// TODO
}
