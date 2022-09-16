package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/r-che/dfi/agent/internal/cfg"
	"github.com/r-che/dfi/agent/internal/cleanup"
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

func waitSignals(dbc *dbi.DBController) {
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
				log.W("Received %q - stopping application...", s)

				// Need to stop all watchers
				fswatcher.StopWatchers()
				// Need to stop database controller
				dbc.Stop()

				return

			case s := <-chReLogs:
				log.I("Received %q - reopening log file...", s)
				if err := log.Reopen(); err != nil {
					log.E("Cannot reopen logs: %v", err)
				} else {
					log.I("Log file reopened")
				}

			case s := <-chReInd:
				log.W("Received %q, starting re-indexing operation...", s)
				// Need to restart watching on configured directories

				// Stop all watchers
				fswatcher.StopWatchers()

				if err = fswatcher.InitWatchers(cfg.Config().IdxPaths, dbc.Channel(), fswatcher.DoReindex); err != nil {
					log.F("Reindexing failed: %v", err)
				}

			case s := <-chClean:
				log.I("Received %q signal - starting cleaning up...", s)
				if err := cleanup.Run(); err != nil {
					log.E("Cannot start cleanup operation: %v", err)
				}
			case <-chStat:
				log.W("TODO: dump stat")
			case <-chStopOps:
				log.W("TODO: stop long term")
		}
	}
}
