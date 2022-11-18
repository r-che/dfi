package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/dfiagent/internal/cleanup"
	"github.com/r-che/dfi/dfiagent/internal/fswatcher"

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

type signalsHandler struct {
	// Channels to receive signals from OS
	chStopApp	chan os.Signal
	chReLogs	chan os.Signal
	chReInd		chan os.Signal
	chClean		chan os.Signal
	chStopOps	chan os.Signal

	// Pointers to controlled objects
	dbc	*dbi.DBController
	wp	*fswatcher.Pool

	// Flags
	reindexRun	bool
	cleanupRun	bool
}

func newSignalsHandler(dbc *dbi.DBController, wp *fswatcher.Pool) *signalsHandler {
	sh := signalsHandler{
		dbc:	dbc,
		wp:		wp,
	}

	sh.chStopApp = make(chan os.Signal, 1)		// Stop application
	signal.Notify(sh.chStopApp, sigTerm, sigInt)

	sh.chReLogs = make(chan os.Signal, 1)		// Reopen logs
	signal.Notify(sh.chReLogs, sigHup)

	sh.chReInd = make(chan os.Signal, 1)		// Run reindexing
	signal.Notify(sh.chReInd, sigUsr1)

	sh.chClean = make(chan os.Signal, 1)		// Run cleanup
	signal.Notify(sh.chClean, sigUsr2)

	sh.chStopOps = make(chan os.Signal, 1)		// Stop long-term operations (reindexing/cleanup)
	signal.Notify(sh.chStopOps, sigQuit)

	// FIXME sh.chStat := make(chan os.Signal, 1)	// Dump statistic to logs
	// FIXME signal.Notify(chStat, sigCont)


	return &sh
}

func (sh *signalsHandler) wait() {
	// Wait signals from OS
	for {
		select {
		case s := <-sh.chStopApp:
			// Stop application
			log.W("Received %q - graceful stopping application... To abort immediately repeat the termination signal", s)

			go func() {
				<-sh.chStopApp
				log.F("Aborted because of the second termination signal")
			}()

			// Stop all watchers
			sh.wp.StopWatchers()

			// Stop database controller
			sh.dbc.Stop()

			return

		case s := <-sh.chReLogs:
			log.I("Received %q - reopening log file...", s)
			if err := log.Reopen(); err != nil {
				log.E("Cannot reopen logs: %v", err)
			} else {
				log.I("Log file reopened")
			}

		// Need to restart watching on configured directories
		case s := <-sh.chReInd:
			log.W("Received %q, starting re-indexing operation...", s)

			sh.startReindex()

		case s := <-sh.chClean:
			log.I("Received %q signal - starting cleaning up...", s)

			sh.startCleanup()

		// FIXME Will be implemented later
		// FIXME case <-sh.chStat:
		// FIXME 	log.W("STUB: dump stat")

		case <-sh.chStopOps:
			sh.wp.TermLong()
			sh.dbc.TermLong()
		}
	}
}

func (sh *signalsHandler) startReindex() {
	// Need to restart watching on configured directories

	if sh.reindexRun {
		log.E("Reindexing has already begun")
		return
	}
	sh.reindexRun = true

	go func() {
		// Stop all watchers
		log.I("Stopping watchers to restart indexing...")
		sh.wp.StopWatchers()

		log.I("Restarting indexing")
		if err := sh.wp.StartWatchers(fswatcher.DoReindex); err != nil {
			log.E("Reindexing failed: %v", err)
		}

		sh.reindexRun = false
	}()
}

func (sh *signalsHandler) startCleanup() {
	if sh.cleanupRun {
		log.E("Cleanup has already begun")
		return
	}
	sh.cleanupRun = true

	go func() {
		if err := cleanup.Run(); err != nil {
			log.E("Cannot start cleanup operation: %v", err)
		}

		sh.cleanupRun = false
	}()
}
