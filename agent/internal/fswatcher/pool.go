package fswatcher

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"
	"sort"
	"os"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"

	fsn "github.com/fsnotify/fsnotify"
)

const (
	// Do or do not reindexing of path
	DoReindex	=	true
	NoReindex	=	false
)

type eventsMap map[string]*types.FSEvent
type doneChan chan interface{}

type Pool struct {
	m		sync.Mutex

	// Preconfigured data
	paths	[]string					// configured paths for pool
	dbChan	chan<- []*dbms.DBOperation	// to send operations to DB controller
	fDelay	time.Duration				// flushing delay

	// Runtime data
	watchers map[string]doneChan
	wg sync.WaitGroup			// to wait until all watchers will be stopped
	termLongVal int				// should be incremented when need to terminate long-term operation
	stop	doneChan			// close this channel to stop all started watchers
}

func NewPool(paths []string, dbChan chan<- []*dbms.DBOperation, flushDelay time.Duration) *Pool {
	return &Pool{
		paths:	paths,
		dbChan:	dbChan,
		fDelay:	flushDelay,
	}
}

func (p *Pool) StartWatchers(doIndexing bool) error {
	// Set lock for all operations with watchers map
	p.m.Lock()
	defer p.m.Unlock()

	// Check for non-empty watchers map
	if len(p.watchers) != 0 {
		return fmt.Errorf("(WatchersPool) watchers already started for this pool (paths: %v)", p.paths)
	}

	// Init watchers map
	p.watchers = make(map[string]doneChan, len(p.paths))

	// Init stop channel
	p.stop = make(chan interface{})

	// Run watchers in parallel
	started := make(chan interface{}, len(p.paths))
	for _, path := range p.paths {
		go func(path string) {
			if done, err := p.newWatcher(path, doIndexing); err != nil {
				log.E("(WatcherPool) Cannot create watcher for %q: %v", path, err)
			} else {
				p.watchers[path] = done
			}

			// Notify that watcher started (or failed, it does not matter)
			started <-nil
		}(path)
	}

	// Wait for all watchers started
	for _ = range p.paths {
		<-started
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

	// Close stop channel to notify all watchers that work should be stopped
	close(p.stop)

	// Wait for watchers finished
	for _, ch := range p.watchers {
		<-ch
	}

	// Clear watchers map
	p.watchers = nil
}

// TermLong terminates long-term operations on filesystem
func (p *Pool) TermLong() {
	p.m.Lock()
	p.termLongVal++
	p.m.Unlock()
}

func (p *Pool) NWatchers() int {
	p.m.Lock()
	defer p.m.Unlock()

	return len(p.watchers)
}

func (p *Pool) newWatcher(watchPath string, doIndexing bool) (doneChan, error) {
	log.D("(watcher:%s) Starting...", watchPath)
	// Check that watchPath is not absolute
	if !filepath.IsAbs(watchPath) {
		absPath, err := filepath.Abs(watchPath)
		if err != nil {
			return nil, fmt.Errorf(
				"(watcher:%s) cannot convert non-absolue path %q to absolute form: %w", watchPath, watchPath, err)
		}

		log.D("(watcher:%s) Converted non-absolute path %q to %q", watchPath, watchPath, absPath)
		watchPath = absPath
	}

	// Create new FS watcher
	watcher, err := fsn.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("(watcher:%s) cannot create watcher: %w", watchPath, err)
	}

	// Cached filesystem events
	events := eventsMap{}

	nWatchers := 0
	// Is reindex required?
	if doIndexing {
		log.I("(watcher:%s) Starting reindexing ...", watchPath)
		// Do recursive scan and reindexing
		nWatchers, err = p.scanDir(watcher, watchPath, events, DoReindex)
		if err != nil {
			return nil, fmt.Errorf("(watcher:%s) cannot reindex: %w", watchPath, err)
		}

		log.I("(watcher:%s) Reindexing done", watchPath)
	} else {
		// Run recursive scan without reindexing
		if nWatchers, err = p.scanDir(watcher, watchPath, events, NoReindex); err != nil {
			return nil, fmt.Errorf("(watcher:%s) cannot set watcher: %w", watchPath, err)
		}
	}

	// Make channel to signal pool that watcher done
	done := make(doneChan)
	// Run watcher for watchPath
	go p.watch(watcher, watchPath, events, done)

	// Return no errors, success
	log.I("(watcher:%s) Started, %d watchers were set", watchPath, nWatchers)

	return done, nil
}

func (p *Pool) watch(watcher *fsn.Watcher, watchPath string, events eventsMap, done doneChan) {
	// On exit from function - close done channel to signal that this instance finished
	defer close(done)

	// Timer to flush cache to database
	timer := time.Tick(p.fDelay)

	for {
		select {
		// Some event
		case event, ok := <-watcher.Events:
			if !ok {
				log.F("(watcher:%s) Filesystem events channel unexpectedly closed", watchPath)
			}

			// Handle event
			p.handleEvent(watcher, &event, events)

		// Need to flush cache
		case <-timer:
			if len(events) == 0 {
				log.D("(watcher:%s) No new events", watchPath)
				// No new events
				continue
			}

			log.D("(watcher:%s) Flushing %d event(s)", watchPath, len(events))

			// Flush collected events
			if err := p.flushCached(watchPath, events); err != nil {
				log.E("(watcher:%s) Cannot flush cached items: %v", watchPath, err)
			}
			// Replace cache by new empty map
			events = eventsMap{}

		// Some error
		case err, ok := <-watcher.Errors:
			if !ok {
				log.F("(watcher:%s) Errors channel unexpectedly closed", watchPath)
			}
			log.E("(watcher:%s) Filesystem events watcher returned error: %v", watchPath, err)

		// Stop watching
		case <-p.stop:
			watcher.Close()
			log.D("(watcher:%s) Watching stopped", watchPath)

			// Flush collected events
			if len(events) != 0 {
				log.I("(watcher:%s) Flushing %d event(s) before termination", watchPath, len(events))

				// Flush collected events
				if err := p.flushCached(watchPath, events); err != nil {
					log.E("(watcher:%s) Cannot flush cached items: %v", watchPath, err)
				}
			}

			log.I("Stopped watcher due to request for %q", watchPath)

			return
		}
	}
}

func (p *Pool) flushCached(watchPath string, events eventsMap) error {
	// Make sorted list of paths
	names := make([]string, 0, len(events))
	for name := range events {
		names = append(names, name)
	}
	sort.Strings(names)

	// Prepare database operations list
	dbOps := make([]*dbms.DBOperation, 0, len(events))

	// Keep current termLongVal value to have ability to compare during long-term operations
	initTermLong := p.termLongVal

	// Handle events one by one
	for _, name := range names {
		// If value of the termLongVal was updated - need to stop long-term operation
		if p.termLongVal != initTermLong {
			return fmt.Errorf("terminated")
		}

		event := events[name]

		switch event.Type {
			// Object was created or updated, need to update database
			case types.EvCreate:
				fallthrough
			case types.EvWrite:
				oInfo, err := getObjectInfo(name)
				if err != nil {
					log.E("(watcher:%s) Skip object %q due to an error in obtaining information about it: %v",
						watchPath, name, err)
					continue
				}
				// Check returned value for empty
				if oInfo == nil {
					// Unsupported object type, just skip it
					log.D("(watcher:%s) Skip object %q due to unsupported type", watchPath, name)
					continue
				}

				// Append a database operation
				dbOps = append(dbOps, &dbms.DBOperation{Op: dbms.Update, ObjectInfo: oInfo})

			// Object was removed from filesystem
			case types.EvRemove:
				// Append database removal operation
				dbOps = append(dbOps, &dbms.DBOperation{Op: dbms.Delete, ObjectInfo: &types.FSObject{FPath: name}})
			default:
				panic(fmt.Sprintf("(watcher:%s) Unhandled FSEvent type %v (%d) occurred on path %q",
					watchPath, event.Type, event.Type, name))
		}
	}

	log.I("(watcher:%s) Sending %d operations to DB controller\n", watchPath, len(dbOps))
	// Send dbOps to database controller channel
	p.dbChan <-dbOps

	// No errors
	return nil
}

func (p *Pool) scanDir(watcher *fsn.Watcher, dir string, events eventsMap, doIndexing bool) (int, error) {
	// Summary count of watchers
	nWatchers := 0
	// Need to add watcher for this directory
	if err := watcher.Add(dir); err != nil {
		log.E("Cannot add watcher to directory %q: %v", dir, err)
	} else {
		log.D("Added watcher to %q", dir)
		nWatchers++
	}

	// Scan directory to watch all subentries
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nWatchers, fmt.Errorf("cannot read entries of directory %q: %w", dir, err)
	}

	// Keep current termLongVal value to have ability to compare during long-term operations
	initTermLong := p.termLongVal

	for _, entry := range entries {
		// If value of the termLongVal was updated - need to stop long-term operation
		if p.termLongVal != initTermLong {
			return nWatchers, fmt.Errorf("terminated")
		}

		// Create object name as path concatenation of top-directory and entry name
		objName := filepath.Join(dir, entry.Name())

		// Is indexing of objects required?
		if doIndexing {
			// Add each entry as newly created object to update data in DB
			events[objName] = &types.FSEvent{Type: types.EvCreate}
		}

		// Check that the the entry is a directory
		if entry.IsDir() {
			// Do recursively call to scan all directory's subentries
			nw, err := p.scanDir(watcher, objName, events, doIndexing)
			if err != nil {
				log.E("Cannot scan nested directory %q: %v", objName, err)
			}
			nWatchers += nw
		}
	}

	return nWatchers, nil
}

func (p *Pool) handleEvent(watcher *fsn.Watcher, event *fsn.Event, events eventsMap) {
	switch {
	// Filesystem object was created
	case event.Op & fsn.Create != 0:

		log.D("Created object %q", event.Name)

		// Create new entry
		events[event.Name] = &types.FSEvent{Type: types.EvCreate}

		// Check that the created object is a directory
		oi, err := os.Lstat(event.Name)
		if err != nil {
			log.W("Cannot stat() for created object %q: %v", event.Name, err)
			return
		}

		if oi.IsDir() {
			// Need to add watcher for newly created directory
			if err = watcher.Add(event.Name); err != nil {
				log.E("Cannot add watcher to directory %q: %v", event.Name, err)
			} else {
				log.I("Added watcher for %q", event.Name)
				// Do recursive scan and add watchers to all subdirectories
				_, err := p.scanDir(watcher, event.Name, events, DoReindex)
				if err != nil {
					log.E("Cannot scan newly created directory %q: %v", event.Name, err)
				}
			}
		}

	// Data in filesystem object was updated
	case event.Op & fsn.Write != 0:
		// Update existing entry
		events[event.Name] = &types.FSEvent{Type: types.EvWrite}

	// Filesystem object was removed o renamed
	case event.Op & (fsn.Remove | fsn.Rename) != 0:

		log.D("Removed/renamed object %q", event.Name)

		// Remove existing entry
		events[event.Name] = &types.FSEvent{Type: types.EvRemove}

		// XXX The code below is not needed, because the path removed from
		// XXX the disc is automatically removed from the watch list
		//if err := watcher.Remove(event.Name); err != nil {
		//	log.E("Cannot remove watcher from %q: %v", event.Name, err)
		//}

	// Object mode was changed
	case event.Op & fsn.Chmod != 0:
		// Nothing

	// Something else
	default:
		// Unexpected event
		log.W("Unknown event from fsnotify: %[1]v (%#[1]v)", event)
		return
	}
}
