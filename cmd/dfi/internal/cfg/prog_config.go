package cfg

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
)

const anyVal = "any"

type progConfig struct {
	// Flag values
	Search	bool
	Show	bool
	Set		bool
	Del		bool
	// TODO Admin	bool

	// Search mode options
	strMtime	string
	strSize		string
	oTypes		string
	csums		string
	hosts		string
	aiiFields	string
	ShowOnlyIds	bool
	ShowID		bool
	HostGroups	bool
	SearchDupes	bool

	// Set mode options
	NoNL		bool

	// Show mdoe options

	//
	// Common options
	//
	types.CommonFlags
	SetAdd		bool
	OneLine		bool
	JSONOut		bool

	//
	// Other options
	//

	// Auxiliary options
	Docs		bool
	Debug		bool
	NoLogTS		bool
	Quiet		bool
	confPath	string

	// Non-flags arguments from command line
	CmdArgs []string

	//
	// Internal filled options
	//

	// Query arguments
	QA			*dbms.QueryArgs
	// Program configuration loaded from file
	fConf		fileCfg
}

func NewConfig() *progConfig {	//nolint:revive	// Currently, I prefer to keep it unexported
	return &progConfig{
		QA: &dbms.QueryArgs{},
	}
}

func (pc *progConfig) DBConfig() *dbms.DBConfig {
	return pc.fConf.DB
}

func (pc *progConfig) NeedID() bool {
	return pc.ShowID || pc.ShowOnlyIds
}

func (pc *progConfig) clone() *progConfig {
	rv := *pc

	// Make deep copy of CmdArgs
	rv.CmdArgs = make([]string, len(pc.CmdArgs))
	copy(rv.CmdArgs, pc.CmdArgs)

	if pc.QA != nil {
		rv.QA = pc.QA.Clone()
	}

	return &rv
}

func (pc *progConfig) prepare(cmdArgs []string) error {
	// Keep search phrases
	pc.CmdArgs = cmdArgs

	// Check and prepare mode options
	if err := pc.prepareMode(); err != nil {
		return err
	}

	// Check for incompatible options
	if err := pc.prepareCheckIncompat(); err != nil {
		return err
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

	// Check for options that implicitly enable quiet mode
	if pc.ShowOnlyIds || pc.OneLine || pc.JSONOut {
		// Enable quiet mode
		pc.Quiet = true
	}

	// Load configuration from file and return result
	return pc.loadConf()
}

func (pc *progConfig) prepareMode() error {
	var prepFunc func() error
	// Check mode
	mn := 0
	if pc.Search {
		mn++
		prepFunc = pc.prepareSearch
	}
	if pc.Show {
		mn++
		prepFunc = pc.prepareShow
	}
	if pc.Set {
		mn++
		prepFunc = pc.prepareSet
	}
	if pc.Del {
		mn++
		prepFunc = pc.prepareDel
	}
	// TODO if pc.Admin { mn++ }

	if mn > 1 {
		return fmt.Errorf("only one mode option can be set")
	}

	if mn == 0 {
		// Use search mode as default
		pc.Search = true
		prepFunc = pc.prepareSearch
	}

	// Prepare required options
	return prepFunc()
}

func (pc *progConfig) prepareCheckIncompat() error {
	io := []string{}	// incompatible options

	// Check for incompatible options
	for k, v := range map[string]bool{
		"deep": pc.QA.DeepSearch,
		"dupes": pc.SearchDupes,
		"only-name": pc.QA.OnlyName,
		"only-tags": pc.QA.OnlyTags,
		"only-descr": pc.QA.OnlyDescr,
	} {
		if v {
			io = append(io, `--` + k)
		}
	}

	// Check for only one or zero incompatible options
	if len(io) <= 1 {
		// OK
		return nil
	}

	return fmt.Errorf("search options are incompatible: %s", strings.Join(io, " "))
}

func (pc *progConfig) prepareSearch() error {
	// Prepare command line arguments to use in the query to DB
	if err := pc.prepareSearchCmdArgs(); err != nil {
		return err
	}


	// Prepare options with arguments
	if err := pc.prepareSearchOptsArgs(); err != nil {
		return err
	}

	//
	// Prepare boolean flags
	//

	// Update values of flags that depend on other flags
	if pc.QA.OnlyDescr {
		pc.CommonFlags.UseDescr = true
	}
	if pc.QA.OnlyTags {
		pc.CommonFlags.UseTags = true
	}

	//
	// Pass common flags from command line to query arguments
	//
	pc.QA.CommonFlags = pc.CommonFlags

	//
	// Check for sufficient conditions for search
	//
	if !pc.QA.CanSearch(pc.CmdArgs) {
		return fmt.Errorf("insufficient arguments to make search")
	}

	// OK
	return nil
}

func (pc *progConfig) prepareSearchCmdArgs() error {
	// Check for required command line arguments
	if (pc.QA.DeepSearch || pc.QA.UseTags || pc.QA.OnlyTags ||
		pc.QA.UseDescr || pc.QA.OnlyDescr || pc.QA.OnlyName || pc.SearchDupes) &&
		len(pc.CmdArgs) == 0 {
		return fmt.Errorf("one of command line options requires at least one command line argument")
	}

	pc.QA.SetSearchPhrases(pc.CmdArgs)

	// OK
	return nil
}

//nolint:cyclop	// Simplifying the code here makes it harder to support
func (pc *progConfig) prepareSearchOptsArgs() error {
	if pc.strMtime != anyVal {
		if err := pc.QA.ParseMtimes(pc.strMtime); err != nil {
			return err
		}
	}

	if pc.strSize != anyVal {
		if err := pc.QA.ParseSizes(pc.strSize); err != nil {
			return err
		}
	}

	if pc.oTypes != anyVal {
		if err := pc.QA.ParseTypes(pc.oTypes, knownTypes); err != nil {
			return err
		}
	}

	if pc.csums != anyVal {
		if err := pc.QA.ParseSums(pc.csums); err != nil {
			return err
		}
	}

	if pc.hosts != anyVal {
		if err := pc.QA.ParseHosts(
			// Convert hostname to lower case to avoid the need for a case-insensitive search in DB
			strings.ToLower(pc.hosts),
		); err != nil {
			return err
		}
	}

	if pc.aiiFields != anyVal {
		if err := pc.QA.ParseAIIFields(pc.aiiFields, dbms.UVAIIFields()); err != nil {
			return err
		}
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

	//nolint:gomnd	// Number of command arguments cannot be lesser than 2
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
	if len(pc.CmdArgs) == 0  && !pc.UseTags {
		return fmt.Errorf("insufficient arguments for --show commands - no object identifiers provided")
	}

	return nil
}
