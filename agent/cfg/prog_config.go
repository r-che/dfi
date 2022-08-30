package cfg

import (
	//"fmt"
	"strings"

	"github.com/r-che/dfi/dbi"
)

type progConfig struct {
	// Flag values

	// Required options
	paths		string			// Hidden option to write original value from the command line
	IdxPaths	[]string
	DBCfg		dbi.DBConfig

	// Other options
	LogFile	string	// Location of log file
	Reindex	bool	// Do reindex on startup

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
	pc.IdxPaths = strings.Split(",", pc.paths)

	// Parsing completed successful
	return nil
}
