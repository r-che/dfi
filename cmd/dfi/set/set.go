package set

import (
	"github.com/r-che/dfi/cmd/dfi/internal/cfg"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
)

func Do(dbc dbms.Client) *types.CmdRV {
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
}

func setTags(dbc dbms.Client, tagsStr string, ids []string) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	log.D("Set tags (append: %t) %q for: %v", c.SetAdd, tagsStr, ids)

	aii := &dbms.AIIArgs{}
	if err := aii.SetTagsStr(tagsStr); err != nil {
		return types.NewCmdRV().AddErr("invalid value from command line: %v", err)
	}

	// Check tags for a special value
	for _, tag := range aii.Tags {
		// Check tag for special value forbidden to set
		if tag == dbms.AIIAllTags {
			return types.NewCmdRV().
				AddErr("tag value %q is a special value that cannot be used as a tag", dbms.AIIAllTags)
		}
	}

	updated, _, err := dbc.ModifyAII(dbms.Update, aii, ids, c.SetAdd)
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
	args := &dbms.AIIArgs{NoNL: c.NoNL}
	args.SetDescr(descr)

	_, updated, err := dbc.ModifyAII(dbms.Update, args, ids, c.SetAdd)
	if err != nil {
		return types.NewCmdRV().
			AddChanged(updated).
			AddErr("cannot set description: %v", err)
	}

	// OK
	return types.NewCmdRV().AddChanged(updated)
}
