package main

import (
	"fmt"
	stdLog "log"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/agent/internal/cfg"
	"github.com/r-che/dfi/agent/internal/fswatcher"

	"github.com/r-che/log"
)

const (
	ProgName		=	`dfiagent`
	ProgNameLong	=	`Distributed File Indexer agent`
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
	log.Open(c.LogFile, ProgName, logFlags)
	log.SetDebug(c.Debug)

	// Starting
	log.I("%s %s started", ProgNameLong, ProgVers)

	log.I("Database host: %s, paths to indexing: %v", c.DBCfg.HostPort, c.IdxPaths)
	// Channel to read information collected by watchers to send it to database
	dbChan := make(chan []*dbi.DBOperation)

	// Init watchers on all configured directories
	doneChan, err := initWatchers(c.IdxPaths, dbChan)
	if err != nil {
		log.F("Cannot initiate watchers on configured paths %v: %v", c.IdxPaths, err)
	}

	// TODO Need to start cleanup goroutine if --reindex set to remove stale records from DB

	// Wait for external events (signals)
	if err = waitEvents(doneChan); err != nil {
		log.F("%v", err)
	}

	// Finish, cleanup operations
	log.I("%s %s finished normally", ProgNameLong, ProgVers)
	log.Close()
}

func initWatchers(paths []string, dbChan chan []*dbi.DBOperation) (chan bool, error) {
	// Create channel to stop watchers
	done := make(chan bool)
	for _, path := range paths {
		if err := fswatcher.New(path, dbChan, done); err != nil {
			return nil, err
		}
	}
	return done, nil
}

func waitEvents(doneChan chan bool) error {
	fmt.Println("Press Enter to STOP")
	fmt.Scanln()
	// Send stop to all watchers
	doneChan <-true

	// Wait for signal processed
	<-doneChan

	return nil
}
