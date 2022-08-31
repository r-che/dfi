package fswatcher

import (
	"fmt"
	"os"
	"time"

	"github.com/r-che/log"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/agent/internal/cfg"

	fsn "github.com/fsnotify/fsnotify"
)

func New(watchPath string, done chan bool) error {
	// Get configuration
	c := cfg.Config()

	// Create new FS watcher
	watcher, err := fsn.NewWatcher()
	if err != nil {
		return fmt.Errorf("(watcher:%s) Cannot create watcher: %v", watchPath, err)
	}

	// Add configured path to watching
	if err = watcher.Add(watchPath); err != nil {
		watcher.Close()
		return fmt.Errorf("(watcher:%s) Cannot add watcher: %v", watchPath, err)
	}

	// Run watcher for watchPath
	go func() {
		// Cached events
		events := map[string]*types.FSEvent{}

		// Timer to flush cache to database
		timer := time.Tick(c.FlushPeriod)

		for {
			select {
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

			// Some event
			case event, ok := <-watcher.Events:
				if !ok {
					log.F("(watcher:%s) Filesystem events channel unexpectedly closed", watchPath)
				}

				// Handle event
				handleEvent(watcher, &event, events)

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
	case event.Op & fsn.Create != 0:
		// Create new entry
		events[event.Name] = &types.FSEvent{Type: types.EvCreate}

		// Check that the created object is a directory
		fi, err := os.Stat(event.Name)
		if err != nil {
			log.W("Cannot stat() for created object %q: %v", event.Name, err)
			return
		}

		if fi.IsDir() {
			// Need to add watcher for newly created directory
			if err = watcher.Add(event.Name); err != nil {
				log.E("Cannot add watcher to directory %q: %v", event.Name, err)
			} else {
				log.I("Added watcher for %q", event.Name)
			}
		}
	case event.Op & fsn.Write != 0:
		// Update existing entry
		events[event.Name] = &types.FSEvent{Type: types.EvWrite}

	case event.Op & (fsn.Remove | fsn.Rename) != 0:
		// Remove existing entry
		events[event.Name] = &types.FSEvent{Type: types.EvRemove}

		// TODO Remove watcher if it is a directory
	case event.Op & fsn.Chmod != 0:
		// Nothing
	default:
		// Unexpected event
		log.W("Unknown event from fsnotify: %[1]v (%#[1]v)", event)
		return
	}
}

func flushCached(events map[string]*types.FSEvent) error {
	for name, event := range events {
		fmt.Printf("%s => %s\n", name, event)
	}

	// No errors
	return nil
}
