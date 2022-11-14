package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/r-che/dfi/dfiagent/internal/cleanup"
	"github.com/r-che/dfi/dfiagent/internal/fswatcher"
	"github.com/r-che/dfi/dbi"

	"github.com/r-che/log"
)

const (
	sigTerm	=	syscall.SIGTERM
	sigInt	=	syscall.SIGINT
	sigHup	=	syscall.SIGHUP
	sigUsr1	=	syscall.SIGUSR1
	sigUsr2	=	syscall.SIGUSR2
	// FIXME sigCont	=	syscall.SIGCONT
	sigQuit	=	syscall.SIGQUIT
)

func waitSignals(dbc *dbi.DBController, wp *fswatcher.Pool) {
	// Create channels for each handled signal

	chStopApp := make(chan os.Signal, 1)	// Stop application
	signal.Notify(chStopApp, sigTerm, sigInt)

	chReLogs := make(chan os.Signal, 1)		// Reopen logs
	signal.Notify(chReLogs, sigHup)

	chReInd := make(chan os.Signal, 1)		// Run reindexing
	signal.Notify(chReInd, sigUsr1)

	chClean := make(chan os.Signal, 1)		// Run cleanup
	signal.Notify(chClean, sigUsr2)

	// FIXME chStat := make(chan os.Signal, 1)		// Dump statistic to logs
	// FIXME signal.Notify(chStat, sigCont)

	chStopOps := make(chan os.Signal, 1)	// Stop long-term operations (reindexing/cleanup)
	signal.Notify(chStopOps, sigQuit)

	// Run handling
	var err error
	// Concurency flags
	reindexRun := false
	cleanupRun := false

	for {
		select {
			case s := <-chStopApp:
				// Stop application
				log.W("Received %q - graceful stopping application... To abort immediately repeat the termination signal", s)

				go func() {
					<-chStopApp
					log.F("Aborted because of the second termination signal")
				}()

				// Stop all watchers
				wp.StopWatchers()

				// Stop database controller
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

				if reindexRun {
					log.E("Reindexing has already begun")
					break
				}
				reindexRun = true

				go func() {
					// Stop all watchers
					log.I("Stopping watchers to restart indexing...")
					wp.StopWatchers()

					log.I("Restarting indexing")
					if err = wp.StartWatchers(fswatcher.DoReindex); err != nil {
						log.E("Reindexing failed: %v", err)
					}

					reindexRun = false
				}()

			case s := <-chClean:
				log.I("Received %q signal - starting cleaning up...", s)

				if cleanupRun {
					log.E("Cleanup has already begun")
					break
				}
				cleanupRun = true

				go func() {
					if err := cleanup.Run(); err != nil {
						log.E("Cannot start cleanup operation: %v", err)
					}

					cleanupRun = false
				}()

			// FIXME Will be implemented later
			// FIXME case <-chStat:
			// FIXME 	log.W("STUB: dump stat")

			case <-chStopOps:
				wp.TermLong()
				dbc.TermLong()
		}
	}
}