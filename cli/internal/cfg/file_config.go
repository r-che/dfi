package cfg

import (
	"fmt"
	"os"
	"errors"
	"io/fs"
	"io/ioutil"
	"encoding/json"

	"github.com/r-che/dfi/common/fschecks"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
)

type fileCfg struct {
	DB	*dbms.DBConfig
}

func (pc *progConfig) loadConf() error {
	// Check existing of configuration
	if _, err := os.Stat(pc.confPath); err != nil {
		// Is file not exist?
		if errors.Is(err, fs.ErrNotExist) {
			// OK, treat this as case with an empty configuration
			log.D("No configuration file exists in %q", pc.confPath)
			return nil
		}

		// Something went wrong
		return fmt.Errorf("cannot access program configuration %q: %w", pc.confPath, err)
	}

	log.D("Using program configuration from %q", pc.confPath)

	// Configuration should not be public-readable - check correctness of ownership/permissions
	if err := fschecks.PrivOwnership(pc.confPath); err != nil {
		return fmt.Errorf("failed to check ownership/mode of program configuraton: %w", err)
	}

	// Read configuration file
	data, err := ioutil.ReadFile(pc.confPath)
	if err != nil {
		return fmt.Errorf("cannot read private database configuration: %w", err)
	}

	// Parse JSON, load it to configuration
	if err = json.Unmarshal(data, &pc.fConf); err != nil {
		return fmt.Errorf("cannot decode configuration %q: %w", pc.confPath, err)
	}

	// OK
	return nil
}
