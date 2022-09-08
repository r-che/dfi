package cleanup

import (
	"strings"
	"os"

	"github.com/r-che/log"
	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/agent/internal/cfg"
)

func Run() error {
	// Create new database client
	dbc, err := dbi.NewClient(&cfg.Config().DBCfg)
	if err != nil {
		return err
	}

	// Start cleanup goroutine
	go cleanup(dbc)

	// OK
	return nil
}

func cleanup(dbc dbi.DBClient) {
	log.I("(Cleanup) Started")

	// Load application configuration
	c := cfg.Config()

	// Counters of not configured record and stale (not existing on FS) records
	nc, nx := 0, 0

	// Make a function to filter records that should be deleted from DB
	filter := func(path string) bool {
		// Iterate through list of paths configured for indexing
		for _, confPath := range c.IdxPaths {
			if strings.HasPrefix(path, confPath) {
				// OK, this path inside of configured directories
				goto fsCheck
			}

			// This path is outside of configured directories, append to delete
			log.D("(Cleanup) Path %q does not belong to configured directories", path)
			nc++

			// Skip filesystem check because it does not make sense,
			// just return true because this path SHOULD BE DELETED
			return true
		}

		fsCheck:
		// Check path for existing
		_, err := os.Stat(path)
		if err == nil {
			// OK, path exists, check next => should NOT be deleted
			return false
		}

		// Check a type of the errror
		if os.IsNotExist(err) {
			log.D("(Cleanup) Path %q does not exist on the local filesystem", path)
			nx++

			// Path is not exist, SHOULD BE DELETED from DB
			return true
		}

		log.E("(Cleanup) Cannot check existing of path %q: %v", path, err)
		// Some unexpected error, log it, DO NOT delete the record
		return false
	}

    // Load from database all objects that belong to the current host
	toDel, err := dbc.LoadHostPaths(filter)
	if err != nil {
		log.E("(Cleanup) Cannot load list of objects paths belong to this host: %v", err)
		return
	}

	// Check for stale data
	if nc + nx == 0 {
		log.I("(Cleanup) Nothing to clean")
		return
	}

	log.I("(Cleanup) %d not configured and %d non-existing records found", nc, nx)

	// Delete outdated paths
	for _, path := range toDel {
		if err := dbc.Delete(path); err != nil {
			log.E("(Cleanup) Cannot delete object for path %q: %v", path, err)
		}
	}

	// Commit deletion
	if _, deleted, err := dbc.Commit(); err != nil {
		log.E("(Cleanup) Fail to delete objects from DB: %v", err)
	} else {
		log.I("(Cleanup) Cleaned up %d objects from DB", deleted)
	}
}
