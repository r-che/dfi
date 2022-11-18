package del

import (
	"github.com/r-che/dfi/cmd/dfi/internal/cfg"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
)

func Do(dbc dbms.Client) *types.CmdRV {
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
}

func delTags(dbc dbms.Client, tagsStr string, ids []string) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	rv := types.NewCmdRV()

	log.D("Delete tags %q from: %v", tagsStr, ids)

	args := &dbms.AIIArgs{}
	if err := args.SetTagsStr(tagsStr); err != nil {
		return rv.AddErr("invalid value from command line: %v", err)
	}

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
