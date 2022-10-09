package cfg

import (
	"fmt"
	stdLog "log"
	"strings"
	"os"
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
	p.AddSeparator(`>> Operating mode (only one can be set)`)
	p.AddSeparator(`# NOTE: Use --help to get detailed help about operating modes`)
	p.AddBool(`search`, `enable search mode, used by default if no other modes are set`, &config.Search, false)
	p.AddBool(`show`, `enable show mode`, &config.Show, false)
	p.AddBool(`set`, `enable set mode`, &config.Set, false)
	p.AddBool(`del`, `enable del mode`, &config.Del, false)
	p.AddBool(`admin`, `enable admin mode`, &config.Admin, false)

	// Modes options

	p.AddSeparator(``)
	p.AddSeparator(`>> Search mode options`)
	p.AddSeparator(`# In the options below:`)
	p.AddSeparator(`# - "set of" - means a set of strings separated by a comma (",")`)
	p.AddSeparator(`# - "range of" - use --help to get help about ranges usage`)
	p.AddString(`mtime`, `range of object modification time`, &config.strMtime, anyVal)
	p.AddString(`size`,
		`range of object size, allowed measure units (case does not matter): K,M,G,T,P,E`,
		&config.strSize, anyVal)
	p.AddString(`type`,
		`set of object types, possible values: ` +
		strings.Join(knownTypes, ", "), &config.oTypes, anyVal)
	p.AddString(`checksum`, `set of objects checksums`, &config.csums, anyVal)
	p.AddString(`host`, `set of hosts when object may be located`, &config.hosts, anyVal)
	p.AddString(`aii-filled|F`,
		`set of filled additional information item fields, possible values: ` +
		strings.Join(aiiFields, ", "), &config.aiiFields, anyVal)
	p.AddBool(`only-name|N`, `use only file name to match search phrases`, &config.QA.OnlyName, false)
	p.AddBool(`only-tags|T`,
		`use only tags field to match search phrases, ` +
		`implicitly enables --tags`, &config.QA.OnlyTags, false)
	p.AddBool(`only-descr`,
		`use only description field to match search phrases, implicitly enables --descr`,
		&config.QA.OnlyDescr, false)
	p.AddBool(`deep|D`, `use additional DBMS dependent search features, ` +
		`can slow down search`, &config.QA.DeepSearch, false)
	p.AddBool(`dupes`,
		`search for duplicates, command line arguments will be treated as objects identifiers`,
		&config.SearchDupes, false)
	p.AddBool(`or`, `use OR instead of AND between conditions`, &config.QA.OrExpr, false)
	p.AddBool(`not`, `use negative value of search conditions`, &config.QA.NegExpr, false)
	// Output related options
	p.AddBool(`show-only-ids|I`, `print only identifiers of found objects, ` +
		`implicitly enables --quiet`, &config.ShowOnlyIds, false)
	p.AddBool(`show-ids|i`,
		`print identifier of object at the beginning of the output lines`, &config.ShowID, false)
	p.AddBool(`hosts-groups|G`,
		`group results by host instead of single line sorted output`, &config.HostGroups, false)

	p.AddSeparator(``)
	p.AddSeparator(`>> Set mode options`)
	p.AddBool(`append|A`,
		`append specified data (tags or description) to the object(s)`, &config.SetAdd, false)
	p.AddBool(`no-newline|n`, `use "; " instead of new line to join new value ` +
		`to description (affects --add)`, &config.NoNL, false)

	p.AddSeparator(``)
	p.AddSeparator(`>> Show mode options`)
	p.AddSeparator(`# No special options for this mode`)

	// Other modes common options
	p.AddSeparator(``)
	p.AddSeparator(`>> Options common to several modes`)
	p.AddBool(`one-line|o`, `print information about each object in one line, ` +
		`implicitly enables --quiet`, &config.OneLine, false)
	p.AddBool(`json|j`, `make JSON output, implicitly enables --quiet`, &config.JSONOut, false)
	p.AddBool(`tags|t`, `enable tags-related operations, ` +
		`requires at least one command line argument`, &config.UseTags, false)
	p.AddBool(`descr`, `enable description-related operations, ` +
		`requires at least one command line argument`, &config.UseDescr, false)

	// Auxiliary options
	p.AddSeparator("")
	p.AddSeparator(">> General options")
	p.AddString(`cfg|c`, `path to configuration file`, &config.confPath, progConfigDefault)
	p.AddBool(`debug|d`, `enable debug logging`, &config.Debug, false)
	p.AddBool(`nologts`, `disable log timestamps`, &config.NoLogTS, false)
	p.AddBool(`quiet|q`, `be quiet, do not print additional information`, &config.Quiet, false)
	p.AddBool(`help|H`, `show detailed help`, &config.Help, false)

	// Parse options
	p.Parse()

	if config.Help {
		help(name)
	}

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

func help(name string) {
	fmt.Println(`
>> Search mode usage

 $ ` + name + ` [search condition options...] [search phrases...]

By default search phrases (SP) used to match full path of objects. This means that
will be found objects that contain SP not only in the file name but also in the path.
E.g. we have files "/data/snow/ball.png" and "/data/snowball.png", SP contain
word "snow" - both files will be found. To use only name of files for search 
use option --only-name, in this case only "/data/snow" will be found.

Special cases of using search phrases:
  * --descr - search phrases also matched to all existing descriptions of objects
  * --tags  - search phrases also matched to all existing tags assigned to objects
              Each search phrase treated as a single tag - any transformations, such
              as splitting into separate tags by commas, are NOT performed
  * --dupes - each search phrase treated ONLY as object ID, this means that
              ONLY the identifier field will be matched with the search phrases
`)
/* TODO
   - ranges
*/
	os.Exit(1)
}
