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

	// Starting agent
	log.I("%s %s started", ProgNameLong, ProgVers)
	log.I("Database host: %s, paths to indexing: %v", c.DBCfg.HostPort, c.IdxPaths)

	// Channel to read information collected by watchers to send it to database
	dbChan := make(chan []*dbi.DBOperation)
	// Channel to stop watchers and DB controller
	doneChan := make(chan bool)

	// Init DB controller
	err := initDB(&c.DBCfg, dbChan)
	if err != nil {
		log.F("Cannot initiate database controller: %v", err)
	}

	// Init watchers on all configured directories
	if err = initWatchers(c.IdxPaths, dbChan, doneChan); err != nil {
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

func initDB(dbc *dbi.DBConfig, dbChan chan []*dbi.DBOperation) error {
	return nil
}

func initWatchers(paths []string, dbChan chan []*dbi.DBOperation, doneChan chan bool) error {
	for _, path := range paths {
		if err := fswatcher.New(path, dbChan, doneChan); err != nil {
			return err
		}
	}

	// OK
	return nil
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
