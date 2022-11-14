package fswatcher

import (
	"fmt"
	"path/filepath"
	"time"
	"os"
	"strings"
	"errors"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"

	"github.com/r-che/log"

	fsn "github.com/fsnotify/fsnotify"
)

type (
	eventsMap	map[string]*FSEvent
	ctrlChan	chan bool
)

const (
	// Do or do not reindexing of path
	DoReindex	=	true
	NoReindex	=	false

	// OS dependent path separator
	pathSeparator	=	string(os.PathSeparator)
)

type Watcher struct {
	// Startup variables
	path			string
	flushInterval	time.Duration
	dbChan			chan<- []*dbms.DBOperation	// to send operations to DB controller

	// Runtime variables
	eMap		eventsMap
	ctrlCh		ctrlChan
	watchDirs	map[string]bool
	termLongVal int				// should be incremented when need to terminate long-term operation

	// fsnotify watcher object
	w	*fsn.Watcher
}

func NewWatcher(path string, flushInterval time.Duration,
				dbChan chan<- []*dbms.DBOperation) (*Watcher, error) {
	log.D("(NewWatcher) Creating watcher for %q ...", path)

	// Check that path is not absolute
	if !filepath.IsAbs(path) {
		// Convert it to the absolute value
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf(
				"(NewWatcher) cannot convert non-absolue path %q to the absolute form: %w", path, err)
		}

		// Replace value
		log.D("(NewWatcher) Converted non-absolute path %q to %q", path, absPath)
		path = absPath
	}

	// Create new watcher structure
	w := Watcher{
		path:			path,
		flushInterval:	flushInterval,
		dbChan:			dbChan,
		ctrlCh:			make(ctrlChan),
		eMap:			eventsMap{},
	}

	// Create new FS watcher
	var err error
	w.w, err = fsn.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("(NewWatcher) cannot create watcher for %q: %w", path, err)
	}

	// OK - return created watcher
	return &w, nil
}

func (w *Watcher) Path() string {
	return w.path
}

func (w *Watcher) Watch(doReindex bool) error {
	// Number of total watchers set during Watch execution
	total := 0

	var err error

	// Is reindex required?
	if doReindex {
		log.I("(Watcher:%s) Starting reindexing ...", w.path)

		// Do recursive scan and reindexing
		total, err = w.scanDir(w.path, DoReindex)
		if err != nil {
			return fmt.Errorf("(Watcher:%s) cannot reindex: %w", w.path, err)
		}

		log.I("(Watcher:%s) Reindexing done", w.path)
	} else {
		// Run recursive scan without reindexing
		if total, err = w.scanDir(w.path, NoReindex); err != nil {
			return fmt.Errorf("(Watcher:%s) cannot set watcher: %w", w.path, err)
		}
	}

	// Run watcher for path
	go w.watch()

	log.I("(Watcher:%s) Started, %d watchers were set", w.path, total)

	// OK
	return nil
}

func (w *Watcher) watch() {
	// Create a set of watched directories to be able to remove watchers from removed directories
	wDirs := w.w.WatchList()
	// Map with directories
	w.watchDirs = make(map[string]bool, len(wDirs))
	for _, dir := range wDirs {
		w.watchDirs[dir] = true
	}

	// Timer to flush cache to database
	timer := time.Tick(w.flushInterval)

	//
	// Run events loop
	//
	for {
		select {
		// Some event
		case event, ok := <-w.w.Events:
			if !ok {
				log.F("(Watcher:%s) Filesystem events channel unexpectedly closed", w.path)
			}

			// Handle event
			w.handleEvent(&event)

		// Need to flush cache
		case <-timer:
			if len(w.eMap) == 0 {
				log.D("(Watcher:%s) No new events", w.path)
				// No new events
				continue
			}

			log.D("(Watcher:%s) Flushing %d event(s)", w.path, len(w.eMap))

			// Flush collected events
			if err := w.flushCached(); err != nil {
				log.E("(Watcher:%s) Cannot flush cached items: %v", w.path, err)
			}

			// Replace cache by new empty map
			w.eMap = eventsMap{}

		// Some error
		case err, ok := <-w.w.Errors:
			if !ok {
				log.F("(Watcher:%s) Errors channel unexpectedly closed", w.path)
			}
			log.E("(Watcher:%s) Filesystem events watcher returned error: %v", w.path, err)

		// Control event - need to stop watching
		case <-w.ctrlCh:
			// Stop watching filesystem
			if err := w.w.Close(); err != nil {
				log.E("(Watcher:%s) Cannot close fsnotify watcher: %v", w.path, err)
			} else {
				log.D("(Watcher:%s) fsnotify watcher closed", w.path)
			}

			// Flush collected events
			if len(w.eMap) != 0 {
				log.I("(Watcher:%s) Flushing %d event(s) before termination", w.path, len(w.eMap))

				// Flush collected events
				if err := w.flushCached(); err != nil {
					log.E("(Watcher:%s) Cannot flush cached items: %v", w.path, err)
				}
			}

			log.I("(Watcher:%s) Stopped due to user request", w.path)

			// Notify pool
			go func() {
				w.ctrlCh <-true
			}()

			return
		}
	}
}

func (w *Watcher) flushCached() error {
	// Prepare database operations list
	dbOps := make([]*dbms.DBOperation, 0, len(w.eMap))

	// Keep current termLongVal value to have ability to compare during long-term operations
	initTermLong := w.termLongVal

	// Handle events one by one
	for ePath, event := range w.eMap {
		// If value of the termLongVal was updated - need to stop long-term operation
		if w.termLongVal != initTermLong {
			return fmt.Errorf("(Watcher:%s) terminated", w.path)
		}

		switch event.Type {
		// Object was created or updated, need to update database
		case EvCreate, EvWrite:
			// Get filesystem information about an object
			oInfo, err := getObjectInfo(ePath)
			if err != nil {
				log.E("(Watcher:%s) Skip object %q due to an error in obtaining information about it: %v",
					w.path, ePath, err)
				continue
			}

			// Check returned value for empty
			if oInfo == nil {
				// Unsupported object type, just skip it
				log.D("(Watcher:%s) Skip object %q due to unsupported type", w.path, ePath)
				continue
			}

			// Append a database operation
			dbOps = append(dbOps, &dbms.DBOperation{Op: dbms.Update, ObjectInfo: oInfo})

		// Object was removed from the filesystem
		case EvRemove:
			// Append database removal operation
			dbOps = append(dbOps, &dbms.DBOperation{Op: dbms.Delete, ObjectInfo: &types.FSObject{FPath: ePath}})

		// Set of objects prefixed with name were removed from the filesystem
		case EvRemovePrefix:
			dbOps = append(dbOps, &dbms.DBOperation{Op: dbms.DeletePrefix, ObjectInfo: &types.FSObject{FPath: ePath}})

		// Unsupported FSEvent
		default:
			panic(fmt.Sprintf("(Watcher:%s) Unhandled FSEvent type %v (%d) occurred on path %q",
				w.path, event.Type, event.Type, ePath))
		}
	}

	log.I("(Watcher:%s) Sending %d operations to DB controller\n", w.path, len(dbOps))

	// Send dbOps to database controller channel
	w.dbChan <-dbOps

	// No errors
	return nil
}

func (w *Watcher) scanDir(dir string, doIndexing bool) (int, error) {
	// Total number of watchers set to the dir
	total := 0

	// Add watcher for the directory itself
	if err := w.w.Add(dir); err != nil {
		log.E("(Watcher:%s) Cannot add watcher to directory %q: %v", w.path, dir, err)
	} else {
		log.D("(Watcher:%s) Added watcher to %q", w.path, dir)
		total++
	}

	// Scan directory to watch all subentries
	entries, err := os.ReadDir(dir)
	if err != nil {
		return total, fmt.Errorf("(Watcher:%s) cannot read entries of directory %q: %w", w.path, dir, err)
	}

	// Keep current termLongVal value to have ability to compare during long-term operations
	initTermLong := w.termLongVal

	for _, entry := range entries {
		// If value of the termLongVal was updated - need to stop long-term operation
		if w.termLongVal != initTermLong {
			return total, fmt.Errorf("(Watcher:%s) terminated", w.path)
		}

		// Create object name as path concatenation of the top level directory and the entry name
		objName := filepath.Join(dir, entry.Name())

		// Is indexing of objects required?
		if doIndexing {
			// Add each entry as newly created object to update data in DB
			w.eMap[objName] = &FSEvent{Type: EvCreate}
		}

		// Check that the the entry is a directory
		if entry.IsDir() {
			// Do recursively call to scan all directory subentries
			nw, err := w.scanDir(objName, doIndexing)
			if err != nil {
				log.E("(Watcher:%s) Cannot scan nested directory %q: %v", w.path, objName, err)
			}
			total += nw
		}
	}

	return total, nil
}

func (w *Watcher) handleEvent(event *fsn.Event) {
	//
	// Filesystem object was created
	//
	if event.Has(fsn.Create) {

		// Create new entry
		w.eMap[event.Name] = &FSEvent{Type: EvCreate}

		// Check that the created object is a directory
		oi, err := os.Lstat(event.Name)
		if err != nil {
			log.W("(Watcher:%s) Cannot stat() for created object %q: %v", w.path, event.Name, err)
			return
		}

		isDir := oi.IsDir()
		log.D("(Watcher:%s) Created %s %q", tools.Tern(isDir, "directory", "object"), w.path, event.Name)

		if isDir {
			// Need to add watcher for newly created directory
			if err = w.w.Add(event.Name); err != nil {
				log.E("(Watcher:%s) Cannot add watcher to directory %q: %v", w.path, event.Name, err)
				return
			}

			// Register directory
			w.watchDirs[event.Name] = true

			log.I("(Watcher:%s) Added watcher for %q", w.path, event.Name)
			// Do recursive scan and add watchers to all subdirectories
			_, err := w.scanDir(event.Name, DoReindex)
			if err != nil {
				log.E("(Watcher:%s) Cannot scan newly created directory %q: %v", w.path, event.Name, err)
			}
		}

		return
	}

	//
	// Data in filesystem object was updated
	//
	if event.Has(fsn.Write) {
		// Update existing entry
		w.eMap[event.Name] = &FSEvent{Type: EvWrite}

		return
	}

	//
	// Filesystem object was removed o renamed
	//
	if event.Op & (fsn.Remove | fsn.Rename) != 0 {
		// Is event name empty?
		if event.Name == "" {
			// Event with empty name may be caused by renaming
			return
		}

		isDir := w.watchDirs[event.Name]

		// XXX This message is duplicated when a directory is removed, because we
		// receive an event from the removed one and from its parent directory as well.
		// Currently, fsnotify (v1.6.0) does not distinguish these events:
		// https://github.com/fsnotify/fsnotify/blob/5f8c606accbcc6913853fe7e083ee461d181d88d/backend_inotify.go#L446
		log.D("(Watcher:%s) %s %s %q", w.path,
			tools.Tern(event.Has(fsn.Remove), "Removed", "Renamed"),
			tools.Tern(isDir, "directory", "object"), event.Name)

		// Remove existing entry in DB
		w.eMap[event.Name] = &FSEvent{Type: EvRemove}

		// Is it a directory?
		if isDir {
			// Unregister removed/renamed directory
			delete(w.watchDirs, event.Name)

			// Is it a rename event?
			if event.Op & fsn.Rename != 0 {
				// Remove watcher from the directory itself and from all directories in the dir hierarchy
				if err := w.unwatchDir(event.Name); err != nil {
					log.E("(Watcher:%s) Cannot remove watchers from directory %q with its subdirectories: %v",
						w.path, event.Name, err)
					return
				}
			} else {
				// Nothing to do in this case, because the path removed from
				// the disk is automatically removed from the watch list
			}
		}

		return
	}

	//
	// Object mode was changed
	//
	if event.Has(fsn.Chmod) {
		// Currently, do nothing
		return
	}

	//
	// Unexpected event
	//
	log.W("(Watcher:%s) Unknown event from fsnotify: %[2]v (%#[2]v)", w.path, event)
}

func (w *Watcher) unwatchDir(dir string) error {
	// Counter for successfuly removed watchers
	removed := 0

	log.D("(Watcher:%s) Removing watchers recursively from %q ...", w.path, dir)

	// Need to remove watcher from the directory self
	if err := w.w.Remove(dir); err != nil {
		return fmt.Errorf("(Watcher:%s) unwatch faield for %q: %v", w.path, dir, err)
	}

	// At least one watcher were removed
	removed++

	// Append OS-dependent path separator to end of the directory name
	// to avoid remove watchers prefixed with dir but are not nested to the dir,
	// e.g: if dir=/dir/to/rem, then [/dir/to/rem/1, /dir/to/rem/2] should be
	// removed, but /dir/to/remove should NOT
	dirPref := dir + pathSeparator

	// Going through all watchers and remove that match the dirPref
	for _, wPath := range w.w.WatchList() {
		// Skip non-matching
		if !strings.HasPrefix(wPath, dirPref) {
			continue
		}

		// Remove watcher from this path
		err := w.w.Remove(wPath)
		if err == nil {
			// Success, increase counter and continue
			removed++
			continue
		}

		//
		// Some error occurred
		//

		// Check for non-existing error
		if errors.Is(err, fsn.ErrNonExistentWatch) {
			// It is strange, but not critical, print warning and continue
			log.W("(Watcher:%s) Tried to remove watcher from a directory %q" +
				" where watcher is already removed", w.path, dir)

			continue
		}

		// Unexpected system error, break removal operation
		return fmt.Errorf("(Watcher:%s) unwatch of %q faield: %v", w.path, dir, err)
	}

	log.D("(Watcher:%s) Total %d watchers were removed from %s", w.path, removed, dir)

	// Remove directory prefix from DB
	w.eMap[dirPref] = &FSEvent{Type: EvRemovePrefix}

	// OK
	return nil
}

// TermLong terminates long-term operations on filesystem.
func (w *Watcher) TermLong() {
	w.termLongVal++
}

// Stop starts the watcher termination process. It does not block the caller.
func (w *Watcher) Stop() {
	go func() {
		w.ctrlCh <-true
	}()
}

// Wait blocks the caller until watcher is stopped.
func (w *Watcher) Wait() {
	<-w.ctrlCh
}
