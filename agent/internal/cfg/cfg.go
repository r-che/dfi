package cfg

import (
	"time"

	"github.com/r-che/optsparser"
)

var config progConfig

func Init(name string) {
	// Create new parser
	p := optsparser.NewParser(name,
		// List of required options
		`indexing-paths`,
		`dbhost`,
	)

	// Required options

	// Paths for indexing
	p.AddString(`indexing-paths|I`, `comma separated list of paths for indexing`, &config.paths, "")
	// Database connection information
	p.AddString(`dbhost|D`, `database host or IP address and port in HOST:PORT format`, &config.DBCfg.HostPort, "")

	// Other options
	p.AddString(`log-file|l`, `log file path`, &config.LogFile, "")
	p.AddBool(`reindex|R`, `do reindex configured paths at startup`, &config.Reindex, false)
	p.AddDuration(`flush-period|F`, `period between flushing FS events to database`, &config.FlushPeriod, 5 * time.Second)


	// Auxiliary options
	p.AddBool(`debug|d`, `enable debug logging`, &config.Debug, false)
	p.AddBool(`nologts|N`, `disable log timestamps`, &config.NoLogTS, false)

	// Parse options
	p.Parse()

	// Check and prepare configuration
	if err := config.prepare(); err != nil {
		p.Usage(err.Error())
	}
}

// Config returns a new configuration structure as a copy
// of existing to avoid accidentally modifications
func Config() *progConfig {
	return config.clone()
}
