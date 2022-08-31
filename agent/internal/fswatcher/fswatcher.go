package fswatcher

import (
	"fmt"
	"os"
	"time"
	"sort"
	"path/filepath"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/agent/internal/cfg"

	"github.com/r-che/log"

	fsn "github.com/fsnotify/fsnotify"
)

func New(watchPath string, done chan bool) error {
	// Get configuration
	c := cfg.Config()

	// Create new FS watcher
	watcher, err := fsn.NewWatcher()
	if err != nil {
		return fmt.Errorf("(watcher:%s) cannot create watcher: %v", watchPath, err)
	}

	// Add configured path to watching
	if err = watcher.Add(watchPath); err != nil {
		watcher.Close()
		return fmt.Errorf("(watcher:%s) cannot add watcher: %v", watchPath, err)
	}

	// Cached filesystem events
	events := map[string]*types.FSEvent{}

	// Is reindex required?
	if c.Reindex {
		log.I("Reindexing path %q ...", watchPath)
		// Do recursive scan and add watchers to all subdirectories
		if err = scanDir(watcher, watchPath, events); err != nil {
			log.E("Cannot reindex path %q: %v", watchPath, err)
		} else {
			log.I("Reindexing done for %q", watchPath)
		}
	}

	// Run watcher for watchPath
	go func() {

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
				if err := flushCached(events); err != nil {
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
			case <-done:
				watcher.Close()
				log.D("(watcher:%s) Watching stopped", watchPath)

				// Flush collected events
				if len(events) != 0 {
					log.D("(watcher:%s) Flushing %d event(s) before termination", watchPath, len(events))

					// Flush collected events
					if err := flushCached(events); err != nil {
						log.F("(watcher:%s) Cannot flush cached items: %v", watchPath, err)
					}
				}

				// Notify that watcher finished
				done <-true

				log.I("Stopped watcher due to request for %q", watchPath)
				return
			}
		}
	}()

	// Return no errors, success
	log.I("Started filesystem events watcher for %q", watchPath)

	return nil
}

func handleEvent(watcher *fsn.Watcher, event *fsn.Event, events map[string]*types.FSEvent) {
	switch {
	// Filesystem object was created
	case event.Op & fsn.Create != 0:

		log.D("Created object %q", event.Name)

		// Create new entry
		events[event.Name] = &types.FSEvent{Type: types.EvCreate}

		// Check that the created object is a directory
		oi, err := os.Stat(event.Name)
		if err != nil {
			log.E("Cannot stat() for created object %q: %v", event.Name, err)
			return
		}

		if oi.IsDir() {
			// Need to add watcher for newly created directory
			if err = watcher.Add(event.Name); err != nil {
				log.E("Cannot add watcher to directory %q: %v", event.Name, err)
			} else {
				log.I("Added watcher for %q", event.Name)
				// Do recursive scan and add watchers to all subdirectories
				if err = scanDir(watcher, event.Name, events); err != nil {
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

func scanDir(watcher *fsn.Watcher, dir string, events map[string]*types.FSEvent) error {
	// Scan directory to watch all subentries
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("cannot read entries of directory %q: %v", dir, err)
	}

	for _, entry := range entries {
		// Create object name as path concatenation of top-directory and entry name
		objName := filepath.Join(dir, entry.Name())
		// Add each entry as newsly created object
		events[objName] = &types.FSEvent{Type: types.EvCreate}

		// Check that the the entry is a directory
		if entry.IsDir() {

			// Need to add watcher for this directory
			if err = watcher.Add(objName); err != nil {
				log.E("Cannot add watcher to directory %q: %v", objName, err)
				continue
			}

			log.I("Added watcher for %q", objName)

			// Do recursively call to scan all directorie's subentries
			if err = scanDir(watcher, objName, events); err != nil {
				log.E("Cannot scan nested directory %q: %v", objName, err)
			}
		}

	}

	return nil
}

func flushCached(events map[string]*types.FSEvent) error {
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
					log.E("Cannot get information about object %q: %v - skip it", name, err)
					continue
				}
				// Append database operation
				dbOps = append(dbOps, &dbi.DBOperation{Op: dbi.Update, ObjectInfo: oInfo})

			// Object was removed from filesystem
			case types.EvRemove:
				dbOps = append(dbOps, &dbi.DBOperation{Op: dbi.Delete})
			default:
				panic(fmt.Sprintf("Unhandled FSEvent %v (%d) occurred on path %q", event.Type, event.Type, name))
		}
	}

	// No errors
	return nil
}

func getObjectInfo(name string) (*types.FSObject, error) {
	// TODO Need to get file information to update data in DB
	return nil, nil
}
