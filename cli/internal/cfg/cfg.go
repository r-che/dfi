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
	p.AddBool(`or`, `use OR instead of AND between conditions`, &config.orExpr, false)
	p.AddBool(`not`, `use negative expression`, &config.negExpr, false)
	p.AddBool(`only-name|N`, `use only file name to match search phrases`, &config.onlyName, false)
	p.AddBool(`only-tags|T`, `use only tags field to match search phrases, implicitly enables --tags`, &config.onlyTags, false)
	p.AddBool(`deep|D`, `use additional DBMS dependent features (can slow down)`, &config.deepSearch, false)
	// Output related options
	p.AddBool(`with-ids|I`, `print ID at the beginning of the output lines`, &config.printID, false)
	p.AddBool(`hosts-groups|H`, `group results by host instead of single line sorted output`, &config.hostGroups, false)

	p.AddSeparator(``)
	p.AddSeparator(`>> Set mode options`)
	p.AddBool(`append|A`, `append specified data (tags or description) to the object(s)`, &config.SetAdd, false)
	p.AddBool(`no-newline|n`, `use "; " instead of new line to join existing description with new (affects --add)`, &config.NoNL, false)

	p.AddSeparator(``)
	p.AddSeparator(`>> Show mode options`)
	p.AddBool(`one-line|o`, `print information about each object in one line`, &config.OneLine, false)


	// Other modes common options
	p.AddSeparator(``)
	p.AddSeparator(`>> Options common to several modes`)
	p.AddBool(`tags`, `enable tags-related operations`, &config.UseTags, false)
	p.AddBool(`descr`, `enable description-related operations`, &config.UseDescr, false)
	// TODO --dupe

	// Other options
	//p.AddString(`hostname`, `override real client hostname by provided value`, &config.DBCfg.CliHost, hostname)

	// Auxiliary options
	p.AddSeparator("")
	p.AddSeparator(">> General options")
	p.AddString(`cfg|c`, `path to configuration file`, &config.confPath, progConfigDefault)
	p.AddBool(`debug|d`, `enable debug logging`, &config.Debug, false)
	p.AddBool(`nologts`, `disable log timestamps`, &config.NoLogTS, false)

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
