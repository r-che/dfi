package cfg

import (
	"fmt"
	"strings"
	"time"
	"os"
	"encoding/json"

	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/fschecks"
)

type progConfig struct {
	// Flag values

	// Required options
	paths		string			// Hidden option to write original value from the command line
	IdxPaths	[]string
	DBPrivCfg		string			// Path to file with DBMS-specific private data - username/password, keys and so on
	DBCfg		dbms.DBConfig

	// Other options
	LogFile		string	// Set location of log file
	Reindex		bool	// Start reindex on startup
	Cleanup		bool	// Cleanup database
	FlushPeriod	time.Duration	// Period between flushing FS events to database
	CalcSums	bool	// Caclculate checksums for regular files
	DBReadOnly	bool	// Do not update any information in database
	MaxSumSize	int64	// Maximum size of the file, checksum of which will be calculated

	// Auxiliary options
	Debug		bool
	NoLogTS		bool
}
func (pc *progConfig) clone() *progConfig {
	rv := *pc

	// Make deep copy of paths to indexing
	rv.IdxPaths = make([]string, len(pc.IdxPaths))
	copy(rv.IdxPaths, pc.IdxPaths)

	return &rv
}

func (pc *progConfig) prepare() error {
	// Prepare paths
	pc.IdxPaths = strings.Split(pc.paths, ",")

	// Prepare DB-private data
	if err := pc.loadPriv(); err != nil {
		return err
	}

	// Convert hostname to lower case to avoid the need for a case-insensitive search in DB
	pc.DBCfg.CliHost = strings.ToLower(pc.DBCfg.CliHost)

	// Parsing completed successful
	return nil
}

func (pc *progConfig) loadPriv() error {
	// Return if no private data was set
	if pc.DBPrivCfg == "" {
		// OK
		return nil
	}

	// Check correctness of ownership/permissions of the private file
	if err := fschecks.PrivOwnership(pc.DBPrivCfg); err != nil {
		return fmt.Errorf("failed to check ownership/mode the private configuration of DB: %w", err)
	}

	// Read configuration file
	data, err := os.ReadFile(pc.DBPrivCfg)
	if err != nil {
		return fmt.Errorf("cannot read private database configuration: %w", err)
	}

	// Parse JSON, load it to configuration
	pc.DBCfg.PrivCfg= map[string]any{}
	if err = json.Unmarshal(data, &pc.DBCfg.PrivCfg); err != nil {
		return fmt.Errorf("cannot decode private database configuration %q: %w", pc.DBPrivCfg, err)
	}

	// OK
	return nil
}
