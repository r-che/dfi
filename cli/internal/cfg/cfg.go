package cfg

import (
	stdLog "log"
	//"os"
	"strings"
	"path/filepath"

	"github.com/r-che/log"
	"github.com/r-che/optsparser"
)

var config progConfig

// Defaults
var knownTypes = []string{"reg", "dir", "sym"}
var progConfigSuff = filepath.Join(".dfi", "cli.json")
var progConfigDefault = filepath.Join("${HOME}", progConfigSuff)

func Init(name string) {
	// Create new parser
	p := optsparser.NewParser(name,
		// List of required options
	)

	// Get real hostname
	p.AddSeparator(`>> Opearting mode (only one can be set)`)
	p.AddBool(`search`, `enable search mode, used by default if no other modes are set`, &config.modeSearch, false)
	p.AddBool(`show`, `enable show mode`, &config.modeShow, false)
	p.AddBool(`set`, `enable set mode`, &config.modeSet, false)
	p.AddBool(`del`, `enable del mode`, &config.modeDel, false)
	p.AddBool(`admin`, `enable admin mode`, &config.modeAdmin, false)

	// Modes options

	p.AddSeparator(``)
	p.AddSeparator(`>> Search mode options`)
	p.AddString(`mtime`, `modification time`, &config.strMtime, anyVal)
	p.AddString(`size`, `object size`, &config.strSize, anyVal)
	p.AddString(`type`, `set of object types, possible values: ` + strings.Join(knownTypes, ", "), &config.oTypes, anyVal)
	p.AddString(`checksum`, `set of objects checksums`, &config.csums, anyVal)
	p.AddString(`host`, `set of hosts when object may be located`, &config.hosts, anyVal)
	// TODO --tag
	// TODO --dupe
	// TODO --descr
	p.AddBool(`or`, `use OR instead of AND between conditions`, &config.orExpr, false)
	p.AddBool(`not`, `use negative expression`, &config.negExpr, false)

	// Other modes common options
	p.AddString(`id`, `object unique ID`, &config.ids, anyVal)

	// Other options
	//p.AddString(`hostname`, `override real client hostname by provided value`, &config.DBCfg.CliHost, hostname)

	// Auxiliary options
	p.AddSeparator("")
	p.AddSeparator(">> General options")
	p.AddString(`cfg|c`, `path to configuration file`, &config.confPath, progConfigDefault)
	p.AddBool(`debug|d`, `enable debug logging`, &config.Debug, false)
	p.AddBool(`nologts|N`, `disable log timestamps`, &config.NoLogTS, false)

	// Parse options
	p.Parse()

	// Configure logger
	if !config.NoLogTS {
		log.SetFlags(log.Flags() | stdLog.Ldate | stdLog.Ltime)
	}
	log.SetDebug(config.Debug)

	// Check and prepare configuration
	if err := config.prepare(p.Args()); err != nil {
		p.Usage(err.Error())
	}
}

// Config returns a new configuration structure as a copy
// of existing to avoid accidentally modifications
func Config() *progConfig {
	return config.clone()
}
