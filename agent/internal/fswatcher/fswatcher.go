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

const (
	// Stubs to fill checksum field on special cases
	csTooLarge = `<FILE TOO LARGE>`
	csErrorStub = `<FAIL TO CALCULATE CHECKSUM>`
)

func New(ctx context.Context, watchPath string, dbChan chan<- []*dbi.DBOperation) error {
	// Get configuration
	c := cfg.Config()

	log.D("(watcher:%s) Starting...", watchPath)
	// Check that watchPath is not absolute
	if !filepath.IsAbs(watchPath) {
		absPath, err := filepath.Abs(watchPath)
		if err != nil {
			return fmt.Errorf("cannot convert non-absolue path %q to absolute form: %v", watchPath, err)
		}

		log.D("Converted non-absolute path %q to %q", watchPath, absPath)
		watchPath = absPath
	}

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
		// Get waitgroup from context
		wg := ctx.Value(types.CtxWGWatchers).(*sync.WaitGroup)

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

			log.D("Added watcher for %q", objName)

			// Do recursively call to scan all directorie's subentries
			if err = scanDir(watcher, objName, events); err != nil {
				log.E("Cannot scan nested directory %q: %v", objName, err)
			}
		}

	}

	return nil
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
