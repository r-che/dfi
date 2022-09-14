package fswatcher

import (
	"context"
	"fmt"
	"os"
	"time"
	"sort"
	"path/filepath"
	"io/fs"
	"sync"
	"crypto/sha1"
	"io"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/agent/internal/cfg"

	"github.com/r-che/log"

	fsn "github.com/fsnotify/fsnotify"
)

// Package's types
type eventsMap map[string]*types.FSEvent

const (
	// Stubs to fill checksum field on special cases
	csTooLarge = `<FILE TOO LARGE>`
	csErrorStub = `<FAIL TO CALCULATE CHECKSUM>`

	// Do or do not reindexing of path
	DoReindex	=	true
	NoReindex	=	false
)

// Package's private global variables

// Map with for FS watchers paths
var watchers *types.SyncMap
// Function to stop all running watchers
var stopWatchers context.CancelFunc
// WaitGroup to wait until all watchers will be stopped
var wgWatchers sync.WaitGroup

func InitWatchers(paths []string, dbChan chan<- []*dbi.DBOperation, doIndexing bool) error {
	// Init/clear watchers map
	watchers = types.NewSyncMap()

	// Add number of watchers to waitgroup
	wgWatchers.Add(len(paths))
	// Context, to stop all watchers
	var ctx context.Context
	ctx, stopWatchers = context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, types.CtxWGWatchers, &wgWatchers)

	// Number of watchers successfuly set
	nSet := 0
	for _, path := range paths {
		if err := newWatcher(ctx, path, dbChan, doIndexing); err != nil {
			log.E("(Watcher) Cannot create watcher for %q: %v", path, err)

			// Decrease value of wait group for failed watcher
			wgWatchers.Done()
		} else {
			nSet++
		}
	}

	if nSet == 0 {
		return fmt.Errorf("no watchers set, cannot work")
	}

	log.I("(Watcher) %d watchers set", nSet)

	// OK
	return nil
}

func StopWatchers() {
	log.D("(Watcher) Stopping all watchers...")
	// Stop all watchers
	stopWatchers()
	// Wait for watcher finished
	wgWatchers.Wait()
}

func newWatcher(ctx context.Context, watchPath string, dbChan chan<- []*dbi.DBOperation, doIndexing bool) error {
	log.D("(watcher:%s) Starting...", watchPath)
	// Check that watchPath is not absolute
	if !filepath.IsAbs(watchPath) {
		absPath, err := filepath.Abs(watchPath)
		if err != nil {
			return fmt.Errorf("(watcher:%s) cannot convert non-absolue path %q to absolute form: %v", watchPath, err)
		}

		log.D("(watcher:%s) Converted non-absolute path %q to %q", watchPath, watchPath, absPath)
		watchPath = absPath
	}

	// Create new FS watcher
	watcher, err := fsn.NewWatcher()
	if err != nil {
		return fmt.Errorf("(watcher:%s) cannot create watcher: %v", watchPath, err)
	}

	// Store created fswatcher
	watchers.Set(watchPath, watcher)

	// Cached filesystem events
	events := eventsMap{}

	nWatchers := 0
	// Is reindex required?
	if doIndexing {
		log.I("(watcher:%s) Starting reindexing ...", watchPath)
		// Do recursive scan and reindexing
		nWatchers, err = scanDir(watcher, watchPath, events, DoReindex)
		if err != nil {
			// Remove broken watcher from map
			watchers.Del(watchPath)

			return fmt.Errorf("(watcher:%s) cannot reindex: %v", watchPath, err)
		}

		log.I("(watcher:%s) Reindexing done", watchPath)
	} else {
		// Run recursive scan without reindexing
		if nWatchers, err = scanDir(watcher, watchPath, events, NoReindex); err != nil {
			// Remove broken watcher from map
			watchers.Del(watchPath)

			return fmt.Errorf("(watcher:%s) cannot set watcher: %v", watchPath, err)
		}
	}

	// Run watcher for watchPath
	go watch(ctx, watchPath, events, dbChan)

	// Return no errors, success
	log.I("Started filesystem watcher for %q, %d watchers were set", watchPath, nWatchers)

	return nil
}

func watch(ctx context.Context, watchPath string, events eventsMap, dbChan chan<- []*dbi.DBOperation) {
	// Get configuration
	c := cfg.Config()

	// Get waitgroup from context
	wg := ctx.Value(types.CtxWGWatchers).(*sync.WaitGroup)

	// Get watcher from global watchers map
	watcher := watchers.Val(watchPath).(*fsn.Watcher)

	// Timer to flush cache to database
	timer := time.Tick(c.FlushPeriod)

	for {
		select {
		// Some event
		case event, ok := <-watcher.Events:
			if !ok {
				log.F("(watcher:%s) Filesystem events channel unexpectedly closed", watchPath)
			}

			// Handle event
			handleEvent(watcher, &event, events)

		// Need to flush cache
		case <-timer:
			if len(events) == 0 {
				log.D("(watcher:%s) No new events", watchPath)
				// No new events
				continue
			}

			log.D("(watcher:%s) Flushing %d event(s)", watchPath, len(events))

			// Flush collected events
			if err := flushCached(watchPath, events, dbChan); err != nil {
				log.F("(watcher:%s) Cannot flush cached items: %v", watchPath, err)
			}
			// Replace cache by new empty map
			events = map[string]*types.FSEvent{}

		// Some error
		case err, ok := <-watcher.Errors:
			if !ok {
				log.F("(watcher:%s) Errors channel unexpectedly closed", watchPath)
			}
			log.E("(watcher:%s) Filesystem events watcher returned error: %v", watchPath, err)

		// Stop watching
		case <-ctx.Done():
			watcher.Close()
			log.D("(watcher:%s) Watching stopped", watchPath)

			// Flush collected events
			if len(events) != 0 {
				log.D("(watcher:%s) Flushing %d event(s) before termination", watchPath, len(events))

				// Flush collected events
				if err := flushCached(watchPath, events, dbChan); err != nil {
					log.F("(watcher:%s) Cannot flush cached items: %v", watchPath, err)
				}
			}

			log.I("Stopped watcher due to request for %q", watchPath)

			// Decrease waitgroup conunter to notify main goroutine that this child finished
			wg.Done()

			return
		}
	}
}

func NWatchers() int {
	// Total number of directories under watchers
	sum := 0
	// Counter function
	count := func(k string, v any) {
		sum += len(v.(*fsn.Watcher).WatchList())
	}

	// Run counter functions on watchers map
	watchers.Apply(count)

	return sum
}

func handleEvent(watcher *fsn.Watcher, event *fsn.Event, events map[string]*types.FSEvent) {
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
				_, err := scanDir(watcher, event.Name, events, DoReindex)
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

func scanDir(watcher *fsn.Watcher, dir string, events map[string]*types.FSEvent, doIndexing bool) (int, error) {
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
		return nWatchers, fmt.Errorf("cannot read entries of directory %q: %v", dir, err)
	}

	for _, entry := range entries {
		// Create object name as path concatenation of top-directory and entry name
		objName := filepath.Join(dir, entry.Name())

		// Is indexing of objects required?
		if doIndexing {
			// Add each entry as newly created object to update data in DB
			events[objName] = &types.FSEvent{Type: types.EvCreate}
		}

		// Check that the the entry is a directory
		if entry.IsDir() {
			// Do recursively call to scan all directorie's subentries
			nw, err := scanDir(watcher, objName, events, doIndexing)
			if err != nil {
				log.E("Cannot scan nested directory %q: %v", objName, err)
			}
			nWatchers += nw
		}
	}

	return nWatchers, nil
}

func flushCached(watchPath string, events map[string]*types.FSEvent, dbChan chan<- []*dbi.DBOperation) error {
	// Make sorted list of paths
	names := make([]string, 0, len(events))
	for name := range events {
		names = append(names, name)
	}
	sort.Strings(names)

	// Prepare database operations list
	dbOps := make([]*dbi.DBOperation, 0, len(events))

	// Handle events one by one
	for _, name := range names {
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
				dbOps = append(dbOps, &dbi.DBOperation{Op: dbi.Update, ObjectInfo: oInfo})

			// Object was removed from filesystem
			case types.EvRemove:
				// Append a database operation
				dbOps = append(dbOps, &dbi.DBOperation{Op: dbi.Delete, ObjectInfo: &types.FSObject{FPath: name}})
			default:
				panic(fmt.Sprintf("(watcher:%s) Unhandled FSEvent type %v (%d) occurred on path %q",
					watchPath, event.Type, event.Type, name))
		}
	}

	log.I("(watcher:%s) Sending %d operations to DB controller\n", watchPath, len(dbOps))
	// Send dbOps to database controller channel
	dbChan <-dbOps

	// No errors
	return nil
}

func getObjectInfo(name string) (*types.FSObject, error) {
	// Get agent configuration
	c := cfg.Config()

	// Get object information to update data in DB
	oi, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}

	// Fill filesystem object
	fso := types.FSObject {
		Name:	oi.Name(),
		FPath:	name,
		Size:	oi.Size(),
		MTime:	oi.ModTime().Unix(),
	}

	switch {
		case oi.Mode() & fs.ModeSymlink != 0:
			// Resolve symbolic link value
			if fso.RPath, err = os.Readlink(name); err != nil {
				log.W("Cannot resolve symbolic link object %q to real path: %v", name, err)
			}

			// Assign proper type
			fso.Type = types.ObjSymlink
			// Continue handling
		case oi.IsDir():
			// Assign proper type
			fso.Type = types.ObjDirectory
		case oi.Mode().IsRegular():
			// Assign proper type
			fso.Type = types.ObjRegular

			// Get checksum but only if enabled
			if c.CalcSums {
				if fso.Size <= c.MaxSumSize || c.MaxSumSize == 0 {
					if fso.Checksum, err = calcSum(name); err != nil {
						log.W("Checksum calculation problem: %v", err)
						// Set stub to signal checksum calculation error
						fso.Checksum = csErrorStub
					}
				} else {
					// Set stub because file is too large to calculate checksum
					fso.Checksum = csTooLarge
				}
			} else {
				// Cleanup checksum field
				fso.Checksum = ""
			}
			// Continue handling
		default:
			// Unsupported filesystem object type
			return nil, nil
	}

	return &fso, nil
}

func calcSum(name string) (string, error) {
	log.D("Checksum of %q - calculating...", name)
	// Open file to calculate checksum of its content
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Hash object to calculate sum
	hash := sha1.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}

	log.D("Checksum of %q - done", name)

	// OK
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
