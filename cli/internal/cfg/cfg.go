package cfg

import (
	//"os"

	//"github.com/r-che/log"
	"github.com/r-che/optsparser"
)

var config progConfig

func Init(name string) {
	// Create new parser
	p := optsparser.NewParser(name,
		// List of required options
		//TODO
	)

	// Get real hostname
	/*
	hostname, err := os.Hostname()
	if err != nil {
		log.E("Cannot get hostname of this host: %v - using the fallback value %q", err, fallbackHostname)
		hostname = fallbackHostname
	}
	*/

	p.AddSeparator(">> Opearting modes (only can be set)")
	p.AddBool("search", "enable search mode, used by default if no other modes are set", &config.modeSearch, false)
	p.AddBool("show", "enable show mode", &config.modeShow, false)
	p.AddBool("set", "enable set mode", &config.modeSet, false)
	p.AddBool("del", "enable del mode", &config.modeDel, false)
	p.AddBool("admin", "enable admin mode", &config.modeAdmin, false)

	// Modes options

	p.AddSeparator("")
	p.AddSeparator(">> Search mode options")
//	p.AddString(	// TODO

	// Other options
	//p.AddString(`hostname`, `override real client hostname by provided value`, &config.DBCfg.CliHost, hostname)

	// Auxiliary options
	p.AddSeparator("")
	p.AddSeparator(">> General options")
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
