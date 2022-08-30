package fswatcher

import (
	"fmt"
	//"os"
	"time"

	"github.com/r-che/log"
	"github.com/r-che/dfi/types"

	fsn "github.com/fsnotify/fsnotify"
)

func New(watchPath string, done chan bool) error {
	watcher, err := fsn.NewWatcher()
	if err != nil {
		return fmt.Errorf("Cannot create watcher for %q: %v", watchPath, err)
	}

	if err = watcher.Add(watchPath); err != nil {
		watcher.Close()
		return fmt.Errorf("Cannot add watcher for %q: %v", watchPath, err)
	}

	// Run watcher for watchPath
	go func() {
		// Cached events
		events := map[string]*types.FSEvent{}

		// Timer to flush cache to database
		timer := time.Tick(time.Second * 5)	// TODO Need to be configurable

		for {
			select {
			// Need to flush cache
			case <-timer:
				fmt.Println("Time to flush!")
				if len(events) == 0 {
					fmt.Println("No new events")
					// No new events
					continue
				}

				// Flush collected events
				if err := flushCached(events); err != nil {
					log.F("Cannot flush cached items: %v", err)
				}
				// Replace cache by new empty map
				events = map[string]*types.FSEvent{}

			// Some event
			case event, ok := <-watcher.Events:
				if !ok {
					log.F("Filesystem events channel for path %q unexpectedly closed", watchPath)
				}

				// Handle event
				handleEvent(watcher, &event, events)

			// Some error
			case err, ok := <-watcher.Errors:
				if !ok {
					log.F("Errors channel for path %q unexpectedly closed", watchPath)
				}
				log.E("Filesystem events watcher for path %q returned error: %v", watchPath, err)

			// Stop watching
			case <-done:
				watcher.Close()
				log.D("Watching on %q finished", watchPath)

				// Flush collected events
				if err := flushCached(events); err != nil {
					log.F("Cannot flush cached items: %v", err)
				}

				// Notify that watcher finished
				done <-true

				log.I("Stoped watching on path %q due to request", watchPath)
				return
			}
		}
	}()

	// Return no errors, success
	log.I("Created filesystem events watcher for %q", watchPath)

	return nil
}

func handleEvent(watcher *fsn.Watcher, event *fsn.Event, events map[string]*types.FSEvent) {
	switch {
	case event.Op & fsn.Create != 0:
		// Create new entry
		events[event.Name] = &types.FSEvent{Type: types.EvCreate}

	case event.Op & fsn.Write != 0:
		// Update existing entry
		events[event.Name] = &types.FSEvent{Type: types.EvWrite}

	case event.Op & (fsn.Remove | fsn.Rename) != 0:
		// Remove existing entry
		events[event.Name] = &types.FSEvent{Type: types.EvRemove}
	case event.Op & fsn.Chmod != 0:
		// Nothing
	default:
		// Unexpected event
		log.W("Unknown event from fsnotify: %[1]v (%#[1]v)", event)
		return
	}
	/*
	//log.I("event: %v", event)
	if event.Op & fsn.Write == fsn.Write {
		//log.I("modified file: %#v", event.Name)
	}

	// Check that the file is a directory
	fi, err := os.Stat(event.Name)
	if err != nil {
		//log.W("Cannot stat of file %q: %v", event.Name, err)
		return
	}
	if fi.IsDir() {
		log.I("Add watcher to %q", event.Name)
		if err = watcher.Add(event.Name); err != nil {
			log.E("Cannot add watcher to directory %q: %v", event.Name, err)
		} else {
			log.I("Added watcher to %q", event.Name)
		}
	}
	*/
}

func flushCached(events map[string]*types.FSEvent) error {
	for name, event := range events {
		fmt.Printf("%s => %s\n", name, event)
	}

	// No errors
	return nil
}
