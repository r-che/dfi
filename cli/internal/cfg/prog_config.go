package cfg

import (
	"fmt"
	//"strings"
	//"time"
	//"io/ioutil"
	//"encoding/json"

	"github.com/r-che/dfi/dbi"
)

type progConfig struct {
	// Flag values
	modeSearch	bool
	modeShow	bool
	modeSet		bool
	modeDel		bool
	modeAdmin	bool

	// Search mode options
	strMtime	string
	strSize		string
	sym			string
	csum		string
	id			string
	host		string
	orExp		bool
	negExpr		bool

	// Required options
	DBCfg		dbi.DBConfig

	// Other options

	// Auxiliary options
	Debug		bool
	NoLogTS		bool
}

func (pc *progConfig) clone() *progConfig {
	rv := *pc

	return &rv
}

func (pc *progConfig) prepare() error {
	// Check mode
	mn := 0
	switch {
	case pc.modeSearch:	mn++; fallthrough
	case pc.modeShow:	mn++; fallthrough
	case pc.modeSet:	mn++; fallthrough
	case pc.modeDel:	mn++; fallthrough
	case pc.modeAdmin:	mn++; fallthrough
	default:
	}
	if mn == 0 {
		// Use search mode as default
		pc.modeSearch = true
	} else if mn > 1 {
		return fmt.Errorf("only one mode option can be set")
	}


	// Parsing completed successful
	return nil
}
