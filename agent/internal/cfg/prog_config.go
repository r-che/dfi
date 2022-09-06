package cfg

import (
	"strings"
	"time"

	"github.com/r-che/dfi/dbi"
)

type progConfig struct {
	// Flag values

	// Required options
	paths		string			// Hidden option to write original value from the command line
	IdxPaths	[]string
	DBCfg		dbi.DBConfig

	// Other options
	LogFile		string	// Set location of log file
	Reindex		bool	// Start reindex on startup
	Cleanup		bool	// Cleanup database
	FlushPeriod	time.Duration	// Period between flushing FS events to database
	CalcSums	bool	// Caclculate checksums for regular files
	MaxSumSize	int64	// Maximum size of the file, checksum of which will be calculated

	// Auxiliary options
	Debug		bool
	NoLogTS		bool
}
func (pc *progConfig) clone() *progConfig {
	rv := *pc

	// Make deep copy of paths to indexing
	copy(rv.IdxPaths, pc.IdxPaths)

	return &rv
}

func (pc *progConfig) prepare() error {
	// Prepare paths
	pc.IdxPaths = strings.Split(pc.paths, ",")

	// Parsing completed successful
	return nil
}
