package main

import (
	stdLog "log"

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
	// Initiate configuration
	cfg.Init(ProgName)
	c := cfg.Config()

	// Configure logger
	var logFlags int
	if !c.NoLogTS {
		logFlags = stdLog.Ldate | stdLog.Ltime
	}
	log.Open("", ProgName, logFlags)
	log.SetDebug(c.Debug)

	// Starting
	log.D("==== %s %s started ====", ProgNameLong, ProgVers)
	log.D("database host - %q database identifier - %q", c.DBCfg.HostPort, c.DBCfg.DBID)

	// Finish, cleanup operations
	log.D("%s %s finished normally", ProgNameLong, ProgVers)
	log.Close()
}
