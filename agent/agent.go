package main

import (
	"context"
	"fmt"
	"sync"
	stdLog "log"

	"github.com/r-che/dfi/agent/internal/cfg"
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
	log.I("%s %s started", ProgNameLong, ProgVers)
	log.I("Database host: %s, paths to indexing: %v", c.DBCfg.HostPort, c.IdxPaths)

	// Channel to read information collected by watchers to send it to database
	dbChan := make(chan []*dbi.DBOperation)

	// Wait group to synchronize finish of all watchers
	wgW := sync.WaitGroup{}
	// Waith group to synchronize database controller
	wgC := sync.WaitGroup{}

	// Context, to stop all watchers
	ctxW, cancelW := context.WithCancel(context.Background())
	ctxW = context.WithValue(ctxW, types.CtxWGWatchers, &wgW)
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

	// Add number of watchers to waitgroup
	wgW.Add(len(c.IdxPaths))
	// Init watchers on all configured directories
	if err = initWatchers(ctxW, c.IdxPaths, dbChan); err != nil {
		log.F("Cannot initiate watchers on configured paths %v: %v", c.IdxPaths, err)
	}

	// TODO Need to start cleanup goroutine if --reindex set to remove stale records from DB

	// Wait for external events (signals)
	if err = waitEvents(cancelW, &wgW, cancelC, &wgC); err != nil {
		log.F("%v", err)
	}

	// Finish, cleanup operations
	log.I("%s %s finished normally", ProgNameLong, ProgVers)
	log.Close()
}

func initDB(ctx context.Context, dbc *dbi.DBConfig, dbChan <-chan []*dbi.DBOperation) error {
	// Init database connector
	dbCtrl, err := dbi.InitController(ctx, dbc, dbChan)
	if err != nil {
		return err
	}

	// Run database controller as goroutine
	go dbCtrl()

	// OK
	return nil
}

func initWatchers(ctx context.Context, paths []string, dbChan chan<- []*dbi.DBOperation) error {
	for _, path := range paths {
		if err := fswatcher.New(ctx, path, dbChan); err != nil {
			return err
		}
	}

	// OK
	return nil
}

func waitEvents(cancelW context.CancelFunc, wgW *sync.WaitGroup, cancelC context.CancelFunc, wgC *sync.WaitGroup) error {
	fmt.Println("Press Enter to STOP")
	fmt.Scanln()

	log.D("Stopping all watchers...")
	// Stop all watchers
	cancelW()
	// Wait for watcher finished
	wgW.Wait()

	log.D("Stopping database controller...")
	// Stop database controller
	cancelC()
	// Wait for database controller finished
	wgC.Wait()

	return nil
}
