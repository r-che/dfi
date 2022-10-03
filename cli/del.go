package main

import (
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cli/internal/cfg"

	"github.com/r-che/log"
)

func doDel(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	switch {
		case c.UseTags:
			return delTags(dbc, c.CmdArgs[0], c.CmdArgs[1:])
		case c.UseDescr:
			return delDescr(dbc, c.CmdArgs)
		default:
			panic("unexpected set mode")
	}

	return types.NewCmdRV().AddErr("unreacable code reached")
}

func delTags(dbc dbms.Client, tagsStr string, ids []string) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	rv := types.NewCmdRV()

	log.D("Delete tags %q from: %v", tagsStr, ids)

	// Split tags string and remove empty lines if exists
	tags := strings.Split(tagsStr, ",")
	for i := 0; i < len(tags); {
		// Remove leading/trailing spaces from tag
		tags[i] = strings.TrimSpace(tags[i])

		// Check tag for special value forbidden to set
		if tags[i] == dbms.AIIAllTags {
			// Need to clear all tags, skip processing other tags
			tags = []string{dbms.AIIAllTags}
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
		return rv.AddErr("invalid tags value from command line: %q", tagsStr)
	}

	args := &dbms.AIIArgs{Tags: tags}
	changed, _, err := dbc.ModifyAII(dbms.Delete, args, ids, c.SetAdd)
	if err != nil {
		rv.AddErr("cannot delete tags: %v", err)
	}

	return rv.AddChanged(changed)
}

func delDescr(dbc dbms.Client, ids []string) *types.CmdRV {
	log.D("Delete description for: %v", ids)

	rv := types.NewCmdRV()

	// Trim spaces from description and set it to argumets
	args := &dbms.AIIArgs{Descr: dbms.AIIDelDescr}
	_, updated, err := dbc.ModifyAII(dbms.Delete, args, ids, false)
	if err != nil {
		rv.AddErr("cannot delete description: %v", err)
	}

	// OK
	return rv.AddChanged(updated)
}
