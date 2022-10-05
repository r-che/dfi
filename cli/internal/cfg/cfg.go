package cfg

import (
	stdLog "log"
	"strings"
	"path/filepath"

	"github.com/r-che/log"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/optsparser"
)

var config *progConfig

// Defaults
var knownTypes = []string{types.ObjRegular, types.ObjDirectory, types.ObjSymlink}
var progConfigSuff = filepath.Join(".dfi", "cli.json")
var progConfigDefault = filepath.Join("${HOME}", progConfigSuff)
var aiiFields = []string{dbms.AIIFieldTags, dbms.AIIFieldDescr}

func Init(name string) {
	// Create new parser
	p := optsparser.NewParser(name,
		// List of required options
	)

	config = NewConfig()

	// Get real hostname
	p.AddSeparator(`>> Opearting mode (only one can be set)`)
	p.AddBool(`search`, `enable search mode, used by default if no other modes are set`, &config.Search, false)
	p.AddBool(`show`, `enable show mode`, &config.Show, false)
	p.AddBool(`set`, `enable set mode`, &config.Set, false)
	p.AddBool(`del`, `enable del mode`, &config.Del, false)
	p.AddBool(`admin`, `enable admin mode`, &config.Admin, false)

	// Modes options

	p.AddSeparator(``)
	p.AddSeparator(`>> Search mode options`)
	p.AddString(`mtime`, `modification time`, &config.strMtime, anyVal)
	p.AddString(`size`, `object size`, &config.strSize, anyVal)
	p.AddString(`type`, `set of object types, possible values: ` + strings.Join(knownTypes, ", "), &config.oTypes, anyVal)
	p.AddString(`checksum`, `set of objects checksums`, &config.csums, anyVal)
	p.AddString(`host`, `set of hosts when object may be located`, &config.hosts, anyVal)
	p.AddString(`aii-filled|F`, `set of filled additional information item fields, ` +
								`possible values: ` + strings.Join(aiiFields, ", "), &config.aiiFields, anyVal)
	p.AddBool(`only-name|N`, `use only file name to match search phrases`, &config.QueryArgs.OnlyName, false)
	p.AddBool(`only-tags|T`, `use only tags field to match search phrases, implicitly enables --tags`, &config.QueryArgs.OnlyTags, false)
	p.AddBool(`only-descr`, `use only description field to match search phrases, implicitly enables --descr`, &config.QueryArgs.OnlyDescr, false)
	p.AddBool(`deep|D`, `use additional DBMS dependent features (can slow down)`, &config.QueryArgs.DeepSearch, false)
	p.AddBool(`dupes`, `search for duplicates, command line arguments will be treated as objects identifiers`, &config.SearchDupes, false)
	p.AddBool(`or`, `use OR instead of AND between conditions`, &config.QueryArgs.OrExpr, false)
	p.AddBool(`not`, `use negative expression`, &config.QueryArgs.NegExpr, false)
	// Output related options
	p.AddBool(`show-only-ids|I`, `print only identifiers of found objects, implicitly enables --quiet`, &config.ShowOnlyIds, false)
	p.AddBool(`show-ids|i`, `print identifier of object at the beginning of the output lines`, &config.ShowID, false)
	p.AddBool(`hosts-groups|H`, `group results by host instead of single line sorted output`, &config.HostGroups, false)

	p.AddSeparator(``)
	p.AddSeparator(`>> Set mode options`)
	p.AddBool(`append|A`, `append specified data (tags or description) to the object(s)`, &config.SetAdd, false)
	p.AddBool(`no-newline|n`, `use "; " instead of new line to join existing description with new (affects --add)`, &config.NoNL, false)

	p.AddSeparator(``)
	p.AddSeparator(`>> Show mode options`)
	p.AddSeparator(`# No special options for this mode`)

	// Other modes common options
	p.AddSeparator(``)
	p.AddSeparator(`>> Options common to several modes`)
	p.AddBool(`one-line|o`, `print information about each object in one line, implicitly enables --quiet`, &config.OneLine, false)
	p.AddBool(`tags|t`, `enable tags-related operations, requires at least one command line argument`, &config.UseTags, false)
	p.AddBool(`descr`, `enable description-related operations, requires at least one command line argument`, &config.UseDescr, false)

	// Other options
	//p.AddString(`hostname`, `override real client hostname by provided value`, &config.DBCfg.CliHost, hostname)

	// Auxiliary options
	p.AddSeparator("")
	p.AddSeparator(">> General options")
	p.AddString(`cfg|c`, `path to configuration file`, &config.confPath, progConfigDefault)
	p.AddBool(`debug|d`, `enable debug logging`, &config.Debug, false)
	p.AddBool(`nologts`, `disable log timestamps`, &config.NoLogTS, false)
	p.AddBool(`quiet|q`, `be quiet, do not print additional information`, &config.Quiet, false)

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
