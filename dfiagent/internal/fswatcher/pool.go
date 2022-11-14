package fswatcher

import (
	"fmt"
	"sync"
	"time"

	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
)

type Pool struct {
	m		sync.Mutex

	// Preconfigured data
	paths			[]string					// configured paths for pool
	dbChan			chan<- []*dbms.DBOperation	// to send operations to DB controller
	flushInterval	time.Duration				// interval between flushing events to DB

	// Runtime data
	watchers map[string]*Watcher
}

func NewPool(paths []string, dbChan chan<- []*dbms.DBOperation, flushInterval time.Duration) *Pool {
	return &Pool{
		paths:			paths,
		dbChan:			dbChan,
		flushInterval:	flushInterval,
	}
}

func (p *Pool) StartWatchers(doReindex bool) error {
	// Set lock for all operations with watchers map
	p.m.Lock()
	defer p.m.Unlock()

	// Check for non-empty watchers map
	if len(p.watchers) != 0 {
		return fmt.Errorf("(WatchersPool) watchers already started for this pool (paths: %v)", p.paths)
	}

	// Init watchers map
	p.watchers = make(map[string]*Watcher, len(p.paths))

	// Concurrent run of watchers
	started := make(chan string, len(p.paths))
	for _, path := range p.paths {
		w, err := NewWatcher(path, p.flushInterval, p.dbChan)
		// Check for error
		if err != nil {
			// Skip this path
			log.E("(WatcherPool) Cannot create watcher for %q: %v", path, err)

			continue
		}

		// Add watcher to watchers map
		p.watchers[path] = w

		// Start watching
		go func() {
			if err := w.Watch(doReindex); err == nil {
				// Success, send empty string - no error path
				started <-""
			}  else {
				// Error occurred, send watcher's path as path caused error
				log.E("(WatcherPool) Cannot start watcher: %v", err)
				started <-w.Path()
			}
		}()
	}

	// Wait for all watchers started
	for range p.paths {
		if errPath := <-started; errPath != "" {
			// Remove problematic watcher from watchers map
			delete(p.watchers, errPath)
		}
	}

	if len(p.watchers) == 0 {
		return fmt.Errorf("(WatcherPool) no watchers set, no directories to work")
	}

	log.I("(WatcherPool) %d top-level watchers set", len(p.watchers))

	// OK
	return nil
}

func (p *Pool) StopWatchers() {
	p.m.Lock()
	defer p.m.Unlock()

	if len(p.watchers) == 0 {
		// Already stopped/not started
		return
	}

	log.D("(WatchersPool) Stopping %d watchers...", len(p.watchers))

	// Stop all watchers
	for _, w := range p.watchers {
		w.Stop()
	}

	// Wait until all watchers are finished
	for _, w := range p.watchers {
		w.Wait()
	}

	// Clear watchers map
	p.watchers = nil

	log.D("(WatchersPool) Watchers stopped")
}

// TermLong terminates long-term operations on filesystem
func (p *Pool) TermLong() {
	// Terminate all long-term operations performed by watchers
	p.m.Lock()
	defer p.m.Unlock()

	for _, w := range p.watchers {
		w.TermLong()
	}
}

func (p *Pool) NWatchers() int {
	p.m.Lock()
	defer p.m.Unlock()

	return len(p.watchers)
}
