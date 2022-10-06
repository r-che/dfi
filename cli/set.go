package main

import (
	"strings"

	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cli/internal/cfg"

	"github.com/r-che/log"
)

func doSet(dbc dbms.Client) *types.CmdRV {
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
	return types.NewCmdRV().AddErr("Unreachable code")
}

func setTags(dbc dbms.Client, tagsStr string, ids []string) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	log.D("Set tags (append: %t) %q for: %v", c.SetAdd, tagsStr, ids)

	// Split tags string and remove empty lines if exists
	tags := strings.Split(tagsStr, ",")
	for i := 0; i < len(tags); {
		// Remove leading/trailing spaces from tag
		tags[i] = strings.TrimSpace(tags[i])

		// Check tag for special value forbidden to set
		if tags[i] == dbms.AIIAllTags {
			return types.NewCmdRV().
				AddErr("tag value %q is a special value that cannot be used as a tag", dbms.AIIAllTags)
		}

		// Remove empty tags
		if tags[i] == "" {
			tags = append(tags[:i], tags[i+1:]...)
		} else {
			i++
		}
	}
	if len(tags) == 0 {
		return types.NewCmdRV().AddErr("invalid tags value from command line: %q", tagsStr)
	}

	// Sort and make tags unique
	tags = tools.UniqStrings(tags)

	updated, _, err := dbc.ModifyAII(dbms.Update, &dbms.AIIArgs{Tags: tags}, ids, c.SetAdd)
	if err != nil {
		return types.NewCmdRV().
			AddChanged(updated).
			AddErr("cannot set tags: %v", err)
	}

	// OK
	return types.NewCmdRV().AddChanged(updated)
}

func setDescr(dbc dbms.Client, descr string, ids []string) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	log.D("Set description (append: %t) %q for: %v", c.SetAdd, descr, ids)

	// Trim spaces from description and set it to argumets
	args := &dbms.AIIArgs{Descr: strings.TrimSpace(descr), NoNL: c.NoNL}
	_, updated, err := dbc.ModifyAII(dbms.Update, args, ids, c.SetAdd)
	if err != nil {
		return types.NewCmdRV().
			AddChanged(updated).
			AddErr("cannot set description: %v", err)
	}

	// OK
	return types.NewCmdRV().AddChanged(updated)
}
