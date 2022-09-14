package main

import (
	"context"
	"sync"
	stdLog "log"

	"github.com/r-che/dfi/agent/internal/cfg"
	"github.com/r-che/dfi/agent/internal/cleanup"
	"github.com/r-che/dfi/agent/internal/fswatcher"
	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/types"

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
	log.I("==== %s %s started ====", ProgNameLong, ProgVers)
	log.I("Paths to indexing - %v client hostname - %q database host - %q database identifier - %q",
		c.IdxPaths, c.DBCfg.CliHost, c.DBCfg.HostPort, c.DBCfg.DBID)

	// Channel to read information collected by watchers to send it to database
	dbChan := make(dbi.DBChan)

	// Waith group to synchronize database controller
	wgC := sync.WaitGroup{}

	// Context to stop database controller
	ctxC, cancelC := context.WithCancel(context.Background())
	ctxC = context.WithValue(ctxC, types.CtxWGDBC, &wgC)

	// Add database controller to wait group
	wgC.Add(1)
	// Init DB controller
	err := initDB(ctxC, &c.DBCfg, dbChan)
	if err != nil {
		log.F("Cannot initiate database controller: %v", err)
	}

	// Init watchers on all configured directories
	if err = fswatcher.InitWatchers(c.IdxPaths, dbChan, c.Reindex); err != nil {
		log.F("Cannot initiate watchers on configured paths %q: %v", c.IdxPaths, err)
	}

	// Start cleanup if requested
	if c.Cleanup {
		if err := cleanup.Run(); err != nil {
			log.F("Cannot start cleanup operation: %v", err)
		}
	}

	// Wait for external events (signals)
	if err = waitEvents(cancelC, &wgC, dbChan); err != nil {
		log.F("%v", err)
	}

	// Finish, cleanup operations
	log.I("%s %s finished normally", ProgNameLong, ProgVers)
	log.Close()
}

func initDB(ctx context.Context, dbCfg *dbi.DBConfig, dbChan dbi.DBChan) error {
	// Init database controller
	dbc, err := dbi.NewController(ctx, dbCfg, dbChan)
	if err != nil {
		return err
	}

	// Run database controller as goroutine
	go dbc.Run()

	// OK
	return nil
}

func waitEvents(cancelC context.CancelFunc, wgC *sync.WaitGroup, dbChan dbi.DBChan) error {
	// Wait for OS signals
	waitSignals(dbChan)

	log.D("Stopping database controller...")
	// Stop database controller
	cancelC()
	// Wait for database controller finished
	wgC.Wait()

	// TODO Dump runtime statistic - move to waitSignals()
	log.W("TODO %d watchers were set", fswatcher.NWatchers())

	return nil
}
