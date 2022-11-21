package main

import (
	"fmt"
	"os"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/cmd/dfi/internal/cfg"
	"github.com/r-che/dfi/dbi"

	"github.com/r-che/dfi/cmd/dfi/del"
	"github.com/r-che/dfi/cmd/dfi/search"
	"github.com/r-che/dfi/cmd/dfi/set"
	"github.com/r-che/dfi/cmd/dfi/show"

	"github.com/r-che/log"
)

const (
	ProgName		=	`dfi`
	ProgNameLong	=	`Distributed File Indexer client`
	versMilestone	=	`-alpha.1`
	ProgVers		=	`0.1.0` + versMilestone
)

// Exit codes
const (
	ExitOK			=	0
	// RetUsage		=	1	// used by Usage() function
	ExitWarn		=	2
	ExitErr			=	3
)


func main() {
	// Init logger to print to stderr
	if err := log.Open(log.DefaultLog, ProgName, log.NoFlags); err != nil {
		// Try to print error message as warning to stdout
		fmt.Printf("WARN: Cannot open default log: %v\n", err)
	}

	// Initiate configuration
	cfg.Init(ProgName, ProgNameLong, ProgVers)

	// Starting
	log.D("==== %s %s started ====", ProgNameLong, ProgVers)

	// Loading common configuration
	c := cfg.Config()

	// Check for database configuration
	dbCfg := c.DBConfig()
	if dbCfg == nil {
		log.F("the configuration file has no database connection settings")
	}

	// Init new database client
	dbc, err := dbi.NewClient(dbCfg)
	if err != nil {
		log.F("Cannot initialize database client: %v", err)
	}

	var rv *types.CmdRV

	switch {
	case c.Search:
		rv = search.Do(dbc)
	case c.Show:
		rv = show.Do(dbc)
	case c.Set:
		rv = set.Do(dbc)
	case c.Del:
		rv = del.Do(dbc)
	// TODO case c.Admin:
	// 	err = fmt.Errorf("not implemented")
	default:
		panic("Unexpected application state - no one operating mode are set")
	}

	os.Exit(printStatus(rv))
}

func printStatus(rv *types.CmdRV) int {
	c := cfg.Config()

	// Print warnings if occurred
	for _, w := range rv.Warns() {
		fmt.Fprintf(os.Stderr, "WRN: %s\n", w)
	}

	// Print errors if occurred
	for _, e := range rv.Errs() {
		fmt.Fprintf(os.Stderr, "ERR: %s\n", e)
	}

	if !c.Quiet {
		// All modes except show and search are edit modes
		editMode := !(c.Show || c.Search)
		// The show mode can work in special modes: one-line output and show-tags (instead of objects)
		showSpecialMode := c.Show && (c.OneLine || c.UseTags)

		// Define status prefix
		pref := tools.Tern(rv.OK(), "OK - ", "")

		if editMode {
			fmt.Printf("%s%d changed\n", pref, rv.Changed())
		} else
		// Read-only mode - search or show, skip output in the show special mode
		if !showSpecialMode {
			fmt.Printf("%s%d objects found\n", pref, rv.Found())
		}
	}

	log.D("%s %s finished", ProgNameLong, ProgVers)
	log.Close()

	if rv.OK() {
		return ExitOK	// return OK to OS
	}

	// Something went wrong
	return tools.Tern(len(rv.Errs()) != 0,
		ExitErr,	// errors occurred
		ExitWarn)	// only warnings
}
