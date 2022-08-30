package main

import (
	stdLog "log"

	"github.com/r-che/dfi/agent/cfg"
	"github.com/r-che/log"
)

const (
	ProgName		=	`dfiagent`
	ProgNameLong	=	`Distributed File Indexer agent`
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
	log.Open(c.LogFile, ProgName, logFlags)
	log.SetDebug(c.Debug)

	// Starting
	log.I("%s started", ProgNameLong)

	// Finish, cleanup operations
	log.I("%s finished normally", ProgNameLong)
	log.Close()
}
