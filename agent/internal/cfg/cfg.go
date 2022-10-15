package cfg

import (
	"fmt"
	"time"
	"os"

	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
	"github.com/r-che/optsparser"
)

const authors = "Roman Chebotarev"

const fallbackHostname = `FALLBACK-HOSTNAME`

var config progConfig

func Init(name, nameLong, vers string) {
	// Create new parser
	p := optsparser.NewParser(name,
		// List of required options
		`indexing-paths`,
		`dbhost`,
		`dbid`,
	).SetUsageOnFail(false)	// Disable calling Usage on Parse error to handle returned error by itself

	// Get real hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.E("Cannot get hostname of this host: %v - using the fallback value %q", err, fallbackHostname)
		hostname = fallbackHostname
	}

	// Required options
	p.AddSeparator(`# Required options`)
	// Paths for indexing
	p.AddString(`indexing-paths|I`, `comma-separated list of paths for indexing`, &config.paths, "")
	// Database connection information
	p.AddString(`dbhost|H`,
		`database host or IP address and port in HOST:PORT format`, &config.DBCfg.HostPort, "")
	// Databace indentifier - name, number ans do on
	p.AddString(`dbid|D`, `database identifier - name, number and so on`, &config.DBCfg.ID, "")

	// Other options
	p.AddSeparator(``,
		`# Other options`,
	)
	p.AddString(`db-priv-cfg|P`,
		`path to the file with private data specific to the particular DBMS - user/pass, etc...`,
		&config.DBPrivCfg, "")
	p.AddBool(`db-readonly`,
		`do not perform any database updates (read-only mode), can be used for debugging`,
		&config.DBReadOnly, false)
	p.AddString(`hostname`,
		`override real agent's hostname to the provided value`, &config.DBCfg.CliHost, hostname)
	p.AddString(`log-file|l`, `path to the log file`, &config.LogFile, "")
	p.AddBool(`reindex|R`,
		`perform reindexing of configured paths on startup`, &config.Reindex, false)
	p.AddBool(`cleanup|c`,
		`delete DB records with no existing files on the disk and not matching the configured paths`,
		&config.Cleanup, false)
	p.AddDuration(`flush-period|F`,
		`period between flushing the collected filesystem events to database`,
		&config.FlushPeriod, 5 * time.Second)
	p.AddBool(`checksums|C`,
		`calculate SHA1 sums for regular files, required for duplicates search support.`,
		&config.CalcSums, false)
	p.AddSeparator(`  WARNING: Calculation of the checksum can cause a huge load` +
					` on the disk/CPU and take a long time!`)
	p.AddInt64(`max-checksum-size|M`,
		`maximum size of the file in bytes, the checksum of which can be calculated, 0 - no limits`,
		&config.MaxSumSize, 0)

	// Auxiliary options
	p.AddSeparator(``,
		`# Auxiliary options`,
	)
	p.AddBool(`debug|d`, `enable debug logging`, &config.Debug, false)
	p.AddBool(`nologts|N`, `disable log timestamps`, &config.NoLogTS, false)
	showVer := false
	p.AddBool(`version|V`, `output version and authors information and exit`, &showVer, false)

	// Signals handling information
	p.AddSeparator(``,
		`# Supported signals:`,
		`* TERM, INT - stop application`,
		`* HUP       - reopen log`,
		`* USR1      - run reindexing`,
		`* USR2      - run cleanup`,
		`* QUIT      - stop long-term operations such reindexing, cleanup, etc`,
	)

	//
	// Parse options
	//
	err = p.Parse()

	// Before checking for a parsing error, check if the --version parameter has been passed
	if showVer {
		// Here we can ignore any parsing errors, such as insufficient required options,
		// unexpected arguments and so on. Just show version/authors info and exit
		fmt.Printf("%s (%s) %s\n", nameLong, name, vers)
		fmt.Printf("DBMS backend: %s\n", dbms.Backend)
		fmt.Printf("Written by %s\n", authors)

		// Ok, no need to test error
		os.Exit(0)
	}

	// Now, need to check the parsing error
	if err != nil {
		// Real problem, call Usage with error description
		p.Usage(err)
	}

	// Check and prepare configuration
	if err := config.prepare(); err != nil {
		p.Usage(err)
	}
}

// Config returns a new configuration structure as a copy
// of existing to avoid accidentally modifications
func Config() *progConfig {
	return config.clone()
}
