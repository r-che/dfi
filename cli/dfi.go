package main

import (
	"github.com/r-che/dfi/cli/internal/cfg"
	//"github.com/r-che/dfi/dbi"

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
	// TODO log.D("database host - %q database identifier - %q", c.DBCfg.HostPort, c.DBCfg.DBID)

	// Finish, cleanup operations
	log.D("%s %s finished normally", ProgNameLong, ProgVers)
	log.Close()
}
