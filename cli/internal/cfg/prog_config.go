package cfg

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/r-che/dfi/dbi"

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
	deepSearch	bool
	printID		bool
	hostGroups	bool

	// Other modes common options
	ids			string

	// Other options

	// Auxiliary options
	confPath	string
	Debug		bool
	NoLogTS		bool

	// Non-flags arguments from command line
	cmdArgs []string

	// Internal filled options

	// Query arguments
	qArgs		*dbi.QueryArgs
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

func (pc *progConfig) DBConfig() *dbi.DBConfig {
	return &pc.fConf.DB
}

func (pc *progConfig) QueryArgs() *dbi.QueryArgs {
	return pc.qArgs
}

func (pc *progConfig) clone() *progConfig {
	rv := *pc

	// Make deep copy of cmdArgs
	rv.cmdArgs = make([]string, len(pc.cmdArgs))
	copy(rv.cmdArgs, pc.cmdArgs)

	rv.qArgs = rv.qArgs.Clone()

	return &rv
}

func (pc *progConfig) prepare(cmdArgs []string) error {
	// Keep search phrases
	pc.cmdArgs = cmdArgs

	// Check mode
	mn := 0
	switch {
	case pc.modeSearch:	mn++; fallthrough
	case pc.modeShow:	mn++; fallthrough
	case pc.modeSet:	mn++; fallthrough
	case pc.modeDel:	mn++; fallthrough
	case pc.modeAdmin:	mn++; fallthrough
	default:
	}
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
			// TODO
		case pc.modeSet:
			// TODO
		case pc.modeDel:
			// TODO
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

	// Load configuration from file and return result
	return pc.loadConf()
}

func (pc *progConfig) prepareSearch() error {
	pc.qArgs = dbi.NewQueryArgs(pc.cmdArgs)

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

	pc.qArgs.SetOr(pc.orExpr)
	pc.qArgs.SetNeg(pc.negExpr)
	pc.qArgs.SetDeep(pc.deepSearch)

	// Check for sufficient conditions for search
	if !pc.qArgs.CanSearch(pc.cmdArgs) {
		return fmt.Errorf("insufficient arguments to make search")
	}
	// OK
	return nil
}
