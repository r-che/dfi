package cfg

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
)

const anyVal = "any"

type progConfig struct {
	// Flag values
	modeSearch	bool
	modeShow	bool
	modeSet		bool
	modeDel		bool
	modeAdmin	bool

	// Search mode options
	strMtime	string
	strSize		string
	oTypes		string
	csums		string
	hosts		string
	orExpr		bool
	negExpr		bool
	onlyName	bool
	onlyTags	bool
	onlyDescr	bool
	deepSearch	bool
	OnlyIds		bool
	printID		bool
	hostGroups	bool

	// Set mode options
	NoNL		bool

	// Show mdoe options
	OneLine		bool

	/*
	 * Other modes common options
	 */
	UseTags		bool
	UseDescr	bool
	SetAdd		bool

	/*
	 * Other options
	 */

	// Auxiliary options
	confPath	string
	Debug		bool
	NoLogTS		bool

	// Non-flags arguments from command line
	CmdArgs []string

	// Internal filled options

	// Query arguments
	qArgs		*dbms.QueryArgs
	// Program configuration loaded from file
	fConf		fileCfg

}

func (pc *progConfig) Search() bool {
	return pc.modeSearch
}
func (pc *progConfig) Show() bool {
	return pc.modeShow
}
func (pc *progConfig) Set() bool {
	return pc.modeSet
}
func (pc *progConfig) Del() bool {
	return pc.modeDel
}
func (pc *progConfig) Admin() bool {
	return pc.modeAdmin
}

func (pc *progConfig) PrintID() bool {
	return pc.printID
}

func (pc *progConfig) HostGroups() bool {
	return pc.hostGroups
}

func (pc *progConfig) DBConfig() *dbms.DBConfig {
	return &pc.fConf.DB
}

func (pc *progConfig) QueryArgs() *dbms.QueryArgs {
	return pc.qArgs
}

func (pc *progConfig) clone() *progConfig {
	rv := *pc

	// Make deep copy of CmdArgs
	rv.CmdArgs = make([]string, len(pc.CmdArgs))
	copy(rv.CmdArgs, pc.CmdArgs)

	if pc.qArgs != nil {
		rv.qArgs = pc.qArgs.Clone()
	}

	return &rv
}

func (pc *progConfig) prepare(CmdArgs []string) error {
	// Keep search phrases
	pc.CmdArgs = CmdArgs

	// Check mode
	mn := 0
	if pc.modeSearch { mn++ }
	if pc.modeShow { mn++ }
	if pc.modeSet { mn++ }
	if pc.modeDel { mn++ }
	if pc.modeAdmin { mn++ }
	if mn == 0 {
		// Use search mode as default
		pc.modeSearch = true
	} else if mn > 1 {
		return fmt.Errorf("only one mode option can be set")
	}

	// Prepare required options
	switch {
		case pc.modeSearch:
			if err := pc.prepareSearch(); err != nil {
				return err
			}
		case pc.modeShow:
			if err := pc.prepareShow(); err != nil {
				return err
			}
		case pc.modeSet:
			if err := pc.prepareSet(); err != nil {
				return err
			}
		case pc.modeDel:
			if err := pc.prepareDel(); err != nil {
				return err
			}
		case pc.modeAdmin:
			// TODO
	}

	// Is program configuration was not set?
	if pc.confPath == progConfigDefault {
		// Try to define default path
		if homeEnv, ok := os.LookupEnv(`HOME`); ok {
			pc.confPath = filepath.Join(homeEnv, progConfigSuff)
		} else {
			log.E(`Cannot get value of the "HOME", the default path to the program configuration is not determined`)
		}
	}

	// Do DBMS specific preparations/checks
	if err := pc.prepareDBMS(); err != nil {
		return err
	}

	// Load configuration from file and return result
	return pc.loadConf()
}

func (pc *progConfig) prepareSearch() error {
	pc.qArgs = dbms.NewQueryArgs(pc.CmdArgs)

	if pc.strMtime != anyVal {
		if err := pc.qArgs.ParseMtimes(pc.strMtime); err != nil {
			return err
		}
	}

	if pc.strSize != anyVal {
		if err := pc.qArgs.ParseSizes(pc.strSize); err != nil {
			return err
		}
	}

	if pc.oTypes != anyVal {
		if err := pc.qArgs.ParseTypes(pc.oTypes, knownTypes); err != nil {
			return err
		}
	}

	if pc.csums != anyVal {
		if err := pc.qArgs.ParseSums(pc.csums); err != nil {
			return err
		}
	}

	if pc.hosts != anyVal {
		if err := pc.qArgs.ParseHosts(pc.hosts); err != nil {
			return err
		}
	}

	// TODO Need to refactor this - convert to internal field like "Flags" and pass it directly to qArgs
	pc.qArgs.SetOr(pc.orExpr)
	pc.qArgs.SetNeg(pc.negExpr)
	pc.qArgs.SetOnlyName(pc.onlyName)
	pc.qArgs.SetUseTags(pc.UseTags)
	pc.qArgs.SetOnlyTags(pc.onlyTags)
	pc.qArgs.SetUseDescr(pc.UseDescr)
	pc.qArgs.SetOnlyDescr(pc.onlyDescr)
	pc.qArgs.SetDeep(pc.deepSearch)

	// Check for sufficient conditions for search
	if !pc.qArgs.CanSearch(pc.CmdArgs) {
		return fmt.Errorf("insufficient arguments to make search")
	}
	// OK
	return nil
}

func (pc *progConfig) prepareSet() error {
	// Also --tag or --descr mode have to be provided but not both
	if !pc.UseTags && !pc.UseDescr {
		return fmt.Errorf("set mode requires field which need to be updated: --tags or --descr")
	}
	if pc.UseTags && pc.UseDescr {
		return fmt.Errorf("cannot set --tags and --descr at the same time")
	}

	// Number of command arguments cannot be lesser than 2
	if len(pc.CmdArgs) < 2 {
		return fmt.Errorf("insufficient arguments for --set command")
	}

	return nil
}

func (pc *progConfig) prepareDel() error {
	// Also --tag or --descr mode have to be provided but not both
	if !pc.UseTags && !pc.UseDescr {
		return fmt.Errorf("del mode requires field which need to be processed: --tags or --descr")
	}
	if pc.UseTags && pc.UseDescr {
		return fmt.Errorf("cannot del --tags and --descr at the same time")
	}

	if pc.UseTags && len(pc.CmdArgs) < 2 {
		// Number of command arguments cannot be lesser than 2
		return fmt.Errorf("insufficient arguments for --del --tags command")
	}

	if pc.UseDescr && len(pc.CmdArgs) == 0 {
		// Need at least one identifier to clear description
		return fmt.Errorf("insufficient arguments for --del --descr command")
	}

	return nil
}

func (pc *progConfig) prepareShow() error {
	// Check for list of identifiers exists
	if len(pc.CmdArgs) == 0 {
		return fmt.Errorf("insufficient arguments for --show commands - no object identifiers provided")
	}

	return nil
}
