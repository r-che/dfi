package cfg

import (
	"fmt"
	"os"
	"errors"
	"io/fs"
	"io/ioutil"
	"encoding/json"

	"github.com/r-che/dfi/common/fschecks"
	"github.com/r-che/dfi/dbi"

	"github.com/r-che/log"
)

type fileCfg struct {
	DB	dbi.DBConfig
}

func (pc *progConfig) loadConf() error {
	// Check existing of configuration
	if _, err := os.Stat(pc.progConf); err != nil {
		// Is file not exist?
		if errors.Is(err, fs.ErrNotExist) {
			// OK, treat this as case with an empty configuration
			log.D("No configuration file exists in %q", pc.progConf)
			return nil
		}

		// Something went wrong
		return fmt.Errorf("cannot access program configuration %q: %v", pc.progConf, err)
	}

	log.D("Using program configuration from %q", pc.progConf)

	// Configuration should not be public-readable - check correctness of ownership/permissions
	if err := fschecks.PrivOwnership(pc.progConf); err != nil {
		return fmt.Errorf("failed to check ownership/mode of program configuraton: %v", err)
	}

	// Read configuration file
	data, err := ioutil.ReadFile(pc.progConf)
	if err != nil {
		return fmt.Errorf("cannot read private database configuration: %v", err)
	}

	// Parse JSON, load it to configuration
	if err = json.Unmarshal(data, &pc.fConf); err != nil {
		return fmt.Errorf("cannot decode configuration %q: %v", pc.progConf, err)
	}
	fmt.Printf("cfg: %#v\n", pc.fConf)

	// OK
	return nil
}
