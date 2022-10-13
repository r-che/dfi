package cfg

import (
	"time"
	"os"

	"github.com/r-che/log"
	"github.com/r-che/optsparser"
)

const fallbackHostname = `FALLBACK-HOSTNAME`

var config progConfig

func Init(name string) {
	// Create new parser
	p := optsparser.NewParser(name,
		// List of required options
		`indexing-paths`,
		`dbhost`,
		`dbid`,
	)

	// Get real hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.E("Cannot get hostname of this host: %v - using the fallback value %q", err, fallbackHostname)
		hostname = fallbackHostname
	}

	// Required options

	// Paths for indexing
	p.AddString(`indexing-paths|I`, `comma separated list of paths for indexing`, &config.paths, "")
	// Database connection information
	p.AddString(`dbhost|H`, `database host or IP address and port in HOST:PORT format`, &config.DBCfg.HostPort, "")
	// Databace indentifier - name, number ans do on
	p.AddString(`dbid|D`, `database identifier - name, number and so on`, &config.DBCfg.ID, "")

	// Other options
	p.AddString(`db-priv-cfg|P`, `path to file with DBMS-specific private data - username/passwd, etc...`, &config.DBPrivCfg, "")
	p.AddBool(`db-readonly`, `do not perform any database updates (read-only mode), used for debugging`, &config.DBReadOnly, false)
	p.AddString(`hostname`, `override real client hostname by provided value`, &config.DBCfg.CliHost, hostname)
	p.AddString(`log-file|l`, `log file path`, &config.LogFile, "")
	p.AddBool(`reindex|R`, `do reindex configured paths at startup`, &config.Reindex, false)
	p.AddBool(`cleanup|c`, `delete records without existing files on disk and that do not correspond to configured paths`, &config.Cleanup, false)
	p.AddDuration(`flush-period|F`, `period between flushing FS events to database`, &config.FlushPeriod, 5 * time.Second)
	p.AddBool(`checksums|C`,
		`calculate sha1 summs for all objects. WARNING: this may cause huge disk load and take a long time`,
		&config.CalcSums, false)
	p.AddInt64(`max-checksum-size|M`, `maximum size of the file in bytes, checksum of which will be calculated, 0 - no limits`,
		&config.MaxSumSize, 0)


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
