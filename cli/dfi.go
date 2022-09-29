package main

import (
	"fmt"
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

	var changed int64

	switch {
	case c.Search():
		err = doSearch(dbc)
	case c.Show():
		err = fmt.Errorf("not implemented") // TODO
	case c.Set():
		changed, err = doSet(dbc)
	case c.Del():
		changed, err = doDel(dbc)
	case c.Admin():
		err = fmt.Errorf("not implemented") // TODO
	default:
		panic("Unexpected application state - no one operating mode are set")
	}

	if err == nil {
		fmt.Printf("OK - %d changed\n", changed)
	} else {
		if changed != 0 {
			log.W("%d records were changed", changed)
		}
		log.F("Command error - %v", err)
	}


	log.D("%s %s finished normally", ProgNameLong, ProgVers)
	log.Close()
}
