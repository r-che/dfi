package main

import (
	"fmt"
	"os"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/cli/internal/cfg"
	"github.com/r-che/dfi/dbi"

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
	cfg.Init(ProgName)

	// Starting
	log.D("==== %s %s started ====", ProgNameLong, ProgVers)

	c := cfg.Config()

	// Init new database client
	dbc, err := dbi.NewClient(c.DBConfig())
	if err != nil {
		log.F("Cannot initialize database client: %v", err)
	}

	var rv *types.CmdRV

	switch {
	case c.Search():
		rv = doSearch(dbc)
	case c.Show():
		rv = doShow(dbc)
	case c.Set():
		rv = doSet(dbc)
	case c.Del():
		rv = doDel(dbc)
	case c.Admin():
		err = fmt.Errorf("not implemented") // TODO
	default:
		panic("Unexpected application state - no one operating mode are set")
	}

	os.Exit(printStatus(rv))
}

func printStatus(rv *types.CmdRV) int {
	c := cfg.Config()

	// Print warnings if occurred
	for _, w := range rv.Warns() {
		fmt.Println("WRN:", w)
	}

	// Print errors if occurred
	for _, e := range rv.Errs() {
		fmt.Println("ERR:", e)
	}

	pref := ""
	if rv.OK() {
		pref = "OK - "
	}

	if c.Show() || c.Search() {
		fmt.Printf("%s%d objects found\n", pref, rv.Found())
	} else {
		fmt.Printf("%s%d changed\n", pref, rv.Changed())
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
