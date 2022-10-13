package main

import (
	stdLog "log"

	"github.com/r-che/dfi/agent/internal/cfg"
	"github.com/r-che/dfi/agent/internal/cleanup"
	"github.com/r-che/dfi/agent/internal/fswatcher"
	"github.com/r-che/dfi/dbi"

	"github.com/r-che/log"
)

const (
	ProgName		=	`dfiagent`
	ProgNameLong	=	`Distributed File Indexer agent`
	ProgVers		=	`0.1`
)

func main() {
	// Initiate configuration
	cfg.Init(ProgName, ProgNameLong, ProgVers)
	c := cfg.Config()

	// Configure logger
	var logFlags int
	if !c.NoLogTS {
		logFlags = stdLog.Ldate | stdLog.Ltime
	}
	log.Open(c.LogFile, ProgName, logFlags)
	log.SetDebug(c.Debug)

	// Starting agent
	log.I("==== %s %s started ====", ProgNameLong, ProgVers)
	log.I("Paths to indexing - %v client hostname - %q database host - %q database identifier - %q",
		c.IdxPaths, c.DBCfg.CliHost, c.DBCfg.HostPort, c.DBCfg.ID)

	// Init and run database controller
	dbc, err := dbi.NewController(&c.DBCfg)
	if err != nil {
		log.F("Cannot initiate database controller: %v", err)
	}
	// Check if read-only mode is required
	if c.DBReadOnly {
		dbc.SetReadOnly(true)
	}
	// Run DB controller
	dbc.Run()

	// Create new watchers pool
	wp := fswatcher.NewPool(c.IdxPaths, dbc.Channel(), c.FlushPeriod)

	// Start watchers asynchronously to avoid delays in cleaning and
	// signal processing if the configured directories contain many
	// entries (files, dirs and so on) that can take a long time
	go func() {
		// Start watchers in pool
		if err := wp.StartWatchers(c.Reindex); err != nil {
			log.F("Cannot initiate watchers pool on configured paths %q: %v", c.IdxPaths, err)
		}
	}()

	// Start cleanup if requested
	if c.Cleanup {
		go func() {
			if err := cleanup.Run(); err != nil {
				log.E("Cannot start cleanup operation: %v", err)
			}
		}()
	}

	// Wait for external events (signals)
	waitSignals(dbc, wp)

	// Finish, cleanup operations
	log.I("%s %s finished normally", ProgNameLong, ProgVers)
	log.Close()
}
