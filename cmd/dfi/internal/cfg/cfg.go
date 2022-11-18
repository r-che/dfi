package cfg

import (
	"fmt"
	stdLog "log"
	"os"
	"path/filepath"
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
	"github.com/r-che/optsparser"
)

const authors = "Roman Chebotarev"

var config *progConfig

// Defaults
var progConfigSuff = filepath.Join(".dfi", "cli.json")
var progConfigDefault = filepath.Join("${HOME}", progConfigSuff)

func Init(name, nameLong, vers string) {
	// Create new parser
	p := optsparser.NewParser(name,
		// List of required options
	).SetUsageOnFail(false)

	config = NewConfig()

	// Get real hostname
	p.AddSeparator(`>> Operating mode (only one can be set)`)
	p.AddBool(`search`, `enable search mode, used by default if no other modes are set`, &config.Search, false)
	p.AddBool(`show`, `enable show mode`, &config.Show, false)
	p.AddBool(`set`, `enable set mode`, &config.Set, false)
	p.AddBool(`del`, `enable deletion mode`, &config.Del, false)
	// TODO p.AddBool(`admin`, `enable admin mode`, &config.Admin, false)

	// Modes options

	p.AddSeparator(``,
		`>> Search mode options`,
		`# NOTE: Use "--docs search" to get additional information how to use search`,
		`# In the options below:`,
		`# - "set of" - means a set of strings separated by a comma (",")`,
		`# - "range of" - use "--docs range" to get help about using range of values`,
	)
	p.AddString(`mtime`,
		`range of object modification time, see "--docs timestamp" for details`,
		&config.strMtime, anyVal)
	p.AddString(`size`,
		`range of object size, allowed measure units (case does not matter): K,M,G,T,P,E`,
		&config.strSize, anyVal)
	p.AddString(`type`,
		`set of object types, possible values: ` +
		strings.Join(types.ObjTypes(), ", "), &config.oTypes, anyVal)
	p.AddString(`checksum`, `set of objects checksums`, &config.csums, anyVal)
	p.AddString(`host`, `set of hosts when object may be located`, &config.hosts, anyVal)
	p.AddString(`aii-filled|F`,
		`set of filled additional information item fields, possible values: ` +
		strings.Join(dbms.UVAIIFields(), ", "), &config.aiiFields, anyVal)
	p.AddBool(`only-name|N`,
		`use only object names to match search phrases, use --docs to get detailed explanation`,
		&config.QA.OnlyName, false)
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


	// Set mode opitions
	p.AddSeparator(``,
		`>> Set mode options`,
		`# NOTE: Use "--docs set" to get additional information how use show mode`,
	)
	p.AddBool(`append|A`,
		`append specified data (tags or description) to the object(s)`, &config.SetAdd, false)
	p.AddBool(`no-newline|n`, `use "; " instead of new line to join new value ` +
		`to description (affects --add)`, &config.NoNL, false)

	// Deletion mode opitions
	p.AddSeparator(``,
		`>> Deletion mode options`,
		`#`,
		`# NOTE: Use "--docs del" to get additional information how use deletion  mode`,
		`#`,
		`# No special options for this mode`,
	)

	// Other modes common options
	p.AddSeparator(``,
		`>> Options common to several modes`,
	)
	p.AddBool(`one-line|o`, `print information about each object in one line, ` +
		`implicitly enables --quiet`, &config.OneLine, false)
	p.AddBool(`json|j`, `make JSON output, implicitly enables --quiet`, &config.JSONOut, false)
	p.AddBool(`tags|t`, `enable tags-related operations`, &config.UseTags, false)
	p.AddBool(`descr`, `enable description-related operations, ` +
		`requires at least one command line argument`, &config.UseDescr, false)

	// Auxiliary options
	p.AddSeparator(``,
		`>> General options`,
	)
	p.AddString(`cfg|c`, `path to configuration file`, &config.confPath, progConfigDefault)
	p.AddBool(`debug|d`, `enable debug logging`, &config.Debug, false)
	p.AddBool(`nologts`, `disable log timestamps`, &config.NoLogTS, false)
	p.AddBool(`quiet|q`, `be quiet, do not print additional information`, &config.Quiet, false)
	p.AddBool(`docs`,
		`show detailed documentation, arguments (if any) are treated as documentation topics`,
		&config.Docs, false)
	showVer := false
	p.AddBool(`version|V`, `output version and authors information and exit`, &showVer, false)

	// Parse options
	err := p.Parse()

	// Is version requested? We can show it without error checking
	if showVer {
		// Show version/authors info and exit
		fmt.Printf("%s (%s) %s\n", nameLong, name, vers)
		fmt.Printf("DBMS backend: %s\n", dbms.Backend)
		fmt.Printf("Written by %s\n", authors)
		os.Exit(0)
	}

	// Now, need to check parsing error
	if err != nil {
		// Some problems with command line options
		fmt.Fprintf(os.Stderr, "%s: usage error - %v\n", name, err)
		fmt.Fprintf(os.Stderr, "Try '%s --help' for more information.\n", name)
		os.Exit(1)
	}

	// Is documentation requested?
	if config.Docs {
		// Print requested documentation and exit
		docs(name, nameLong, p.Args())
	}

	// Configure logger
	if !config.NoLogTS {
		if err := log.SetFlags(log.Flags() | stdLog.Ldate | stdLog.Ltime); err != nil {
			panic("Cannot set logger flags: " + err.Error())
		}
	}
	log.SetDebug(config.Debug)

	// Check and prepare configuration
	if err := config.prepare(p.Args()); err != nil {
		// Preparation failed, print error and exit
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		fmt.Fprintf(os.Stderr, "Try '%s --help' for more information.\n", name)
		os.Exit(1)
	}
}

// Config returns a new configuration structure as a copy
// of existing to avoid accidentally modifications
func Config() *progConfig {	//nolint:revive	// Currently, I prefer to keep it unexported
	return config.clone()
}
