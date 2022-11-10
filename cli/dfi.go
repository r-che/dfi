package main

import (
	"fmt"
	"os"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/cli/internal/cfg"
	"github.com/r-che/dfi/dbi"

	// Command Line Interace packages
	"github.com/r-che/dfi/cli/del"
	"github.com/r-che/dfi/cli/search"
	"github.com/r-che/dfi/cli/set"
	"github.com/r-che/dfi/cli/show"

	"github.com/r-che/log"
)

const (
	ProgName		=	`dfi`
	ProgNameLong	=	`Distributed File Indexer client`
	ProgVers		=	`0.1`
)

func main() {
	// Init logger to print to stderr
	log.Open("", ProgName, log.NoFlags)

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
		pref := ""
		if rv.OK() {
			pref = "OK - "
		}

		if c.Show || c.Search {
			// Skip status output in show mode with --one-line or --tags keys
			if c.Show && (c.OneLine || c.UseTags) {
				// Print nothing
			} else {
				fmt.Printf("%s%d objects found\n", pref, rv.Found())
			}
		} else {
			fmt.Printf("%s%d changed\n", pref, rv.Changed())
		}
	}

	log.D("%s %s finished", ProgNameLong, ProgVers)
	log.Close()

	if rv.OK() {
		// OK
		return 0
	}

	// Something went wrong
	if len(rv.Errs()) != 0 {
		return 3
	}

	// Only warnings otherwise
	return 2
}
