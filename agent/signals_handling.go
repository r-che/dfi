package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/r-che/dfi/agent/internal/cfg"
	"github.com/r-che/dfi/agent/internal/fswatcher"
	"github.com/r-che/dfi/dbi"

	"github.com/r-che/log"
)

const (
	sigTerm	=	syscall.SIGTERM
	sigInt	=	syscall.SIGINT
	sigHup	=	syscall.SIGHUP
	sigUsr1	=	syscall.SIGUSR1
	sigUsr2	=	syscall.SIGUSR2
	sigCont	=	syscall.SIGCONT
	sigQuit	=	syscall.SIGQUIT
)

func waitSignals(dbChan dbi.DBChan) {
	// Create channels for each handled signal

	chStopApp := make(chan os.Signal, 0)	// Stop application
	signal.Notify(chStopApp, sigTerm, sigInt)

	chReLogs := make(chan os.Signal, 0)		// Reopen logs
	signal.Notify(chReLogs, sigHup)

	chReInd := make(chan os.Signal, 0)		// Run reindexing
	signal.Notify(chReInd, sigUsr1)

	chClean := make(chan os.Signal, 0)		// Run cleanup
	signal.Notify(chClean, sigUsr2)

	chStat := make(chan os.Signal, 0)		// Dump statistic to logs
	signal.Notify(chStat, sigCont)

	chStopOps := make(chan os.Signal, 0)	// Stop long-term operations (reindexing/cleanup)
	signal.Notify(chStopOps, sigQuit)

	// Run handling
	var err error
	for {
		select {
			case s := <-chStopApp:
				// Stop application
				log.W("Stopping due to signal %q (#%#v)", s, s)

				// Need to stop all watchers
				fswatcher.StopWatchers()

				// TODO Need to move stopDB to this point

				return

			case s := <-chReLogs:
				log.I("Received %q signal - reopening log file...", s)
				if err := log.Reopen(); err != nil {
					log.E("Cannot reopen logs: %v", err)
				} else {
					log.I("Log file reopened")
				}

			case s := <-chReInd:
				log.W("Catched reindexing signal %q, stopping all watchers...", s)
				// Need to restart watching on configured directories

				// Stop all watchers
				fswatcher.StopWatchers()

				c := cfg.Config()
				log.W("Restarting indexing for paths %q...", c.IdxPaths)
				if err = fswatcher.InitWatchers(c.IdxPaths, dbChan, fswatcher.DoReindex); err != nil {
					log.F("Reindexing failed: %v", err)
				}

			case <-chClean:
				log.W("TODO: cleaning up")
			case <-chStat:
				log.W("TODO: dump stat")
			case <-chStopOps:
				log.W("TODO: stop long term")
		}
	}
}
