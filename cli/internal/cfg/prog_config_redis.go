//go:build dbi_redis
package cfg

import (
	"fmt"
	"strings"
)

func (pc *progConfig) prepareDBMS() error {
	// Check for existing of required command line arguments
	if (pc.DeepSearch || pc.UseTags || pc.OnlyTags  || pc.UseDescr || pc.OnlyDescr) && len(pc.CmdArgs) == 0 {
		return fmt.Errorf("(Redis) one of the search options requires an command line argument")
	}

	// Check for incompatible options
	io := make([]string, 0, 3)	// Incompatible options
	for k, v := range map[string]bool{"deep": pc.DeepSearch, "only-name": pc.OnlyName, "only-tags": pc.OnlyTags} {
		if v {
			io = append(io, `--` + k)
		}
	}

	if len(io) < 2 {
		// OK
		return nil
	}

	return fmt.Errorf("(Redis) search options are incompatible: %s", strings.Join(io, " "))
}
