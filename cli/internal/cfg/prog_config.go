package cfg

import (
	"fmt"
	"os"
	"path/filepath"

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
	ids			string
	hosts		string
	orExpr		bool
	negExpr		bool

	// Other options

	// Auxiliary options
	progConf	string
	Debug		bool
	NoLogTS		bool

	// Non-flags arguments from command line
	cmdArgs []string

	// Internal filled options

	// Query arguments
	qArgs		*queryArgs
	// Program configuration loaded from file
	fConf		fileCfg

}

func (pc *progConfig) clone() *progConfig {
	rv := *pc

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
	if pc.progConf == progConfigDefault {
		// Try to define default path
		if homeEnv, ok := os.LookupEnv(`HOME`); ok {
			pc.progConf = filepath.Join(homeEnv, progConfigSuff)
		} else {
			log.E(`Cannot get value of the "HOME", the default path to the program configuration is not determined`)
		}
	}

	// Load configuration from file and return result
	return pc.loadConf()
}

func (pc *progConfig) prepareSearch() error {
	pc.qArgs = &queryArgs{}

	if pc.strMtime != anyVal {
		if err := pc.qArgs.parseMtimes(pc.strMtime); err != nil {
			return err
		}
	}

	if pc.strSize != anyVal {
		if err := pc.qArgs.parseSizes(pc.strSize); err != nil {
			return err
		}
	}

	if pc.oTypes != anyVal {
		if err := pc.qArgs.parseTypes(pc.oTypes); err != nil {
			return err
		}
	}

	if pc.csums != anyVal {
		if err := pc.qArgs.parseSums(pc.csums); err != nil {
			return err
		}
	}

	if pc.ids != anyVal {
		if err := pc.qArgs.parseIDs(pc.ids); err != nil {
			return err
		}
	}

	if pc.hosts != anyVal {
		if err := pc.qArgs.parseHosts(pc.hosts); err != nil {
			return err
		}
	}

	pc.qArgs.setOr(pc.orExpr)
	pc.qArgs.setNeg(pc.negExpr)

	// Check for sufficient conditions for search
	if !pc.qArgs.canSearch(pc.cmdArgs) {
		return fmt.Errorf("insufficient arguments to make search")
	}
	// OK
	return nil
}
