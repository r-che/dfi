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
    // Load from database all objects that belong to the current host
	dbPaths, err := dbc.LoadHostPaths()
	if err != nil {
		log.E("(Cleanup) Cannot load list of objects paths belong to this host: %v", err)
		return
	}

	log.I("(Cleanup) Total %d object paths loaded", len(dbPaths))

	// Load application configuration
	c := cfg.Config()

	// Counters of not configured record and stale (not existing on FS) records
	nc, nx := 0, 0

	PATHS:
	for _, path := range dbPaths {
		// Iterate through list of paths configured for indexing
		for _, confPath := range c.IdxPaths {
			if strings.HasPrefix(path, confPath) {
				// OK, this path under inside of configured directories
				goto fsCheck
			}
			// This path is outside of configured directories, append to delete
			log.D("(Cleanup) Path %q does not belong to configured directories", path)
			nc++
			// Delete it from database
			dbc.Delete(path)
			// Skip filesystem check because it does not make sense
			continue PATHS
		}

		fsCheck:
		// Check path for existing
		_, err := os.Stat(path)
		if err == nil {
			// OK, path exists, check next
			continue
		}

		// Check a type of the errror
		if ! os.IsNotExist(err) {
			// Some unexpected error, log it and continue, DO NOT delete the record
			log.E("(Cleanup) Cannot check existing of path %q: %v", path, err)
			continue
		}

		// Path is not exist, should be deleted from DB
		log.D("(Cleanup) Path %q does not exist on the local filesystem", path)
		dbc.Delete(path)
		nx++
	}

	// Check for stale data
	if nc + nx == 0 {
		log.I("(Cleanup) Nothing to clean")
		return
	}

	log.I("(Cleanup) %d not configured and %d non-existing records found", nc, nx)

	// Commit deletion
	if _, deleted, err := dbc.Commit(); err != nil {
		log.E("(Cleanup) Fail to delete objects from DB: %v", err)
	} else {
		log.I("(Cleanup) Cleaned up %d objects from DB", deleted)
	}
}
