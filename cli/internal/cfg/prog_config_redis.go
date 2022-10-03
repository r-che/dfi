//go:build dbi_redis
package cfg

import (
	"fmt"
	"strings"
)

func (pc *progConfig) prepareDBMS() error {
	// Check for existing of required command line arguments
	if (pc.QueryArgs.DeepSearch || pc.QueryArgs.UseTags || pc.QueryArgs.OnlyTags ||
		pc.QueryArgs.UseDescr || pc.QueryArgs.OnlyDescr) && len(pc.CmdArgs) == 0 {
		return fmt.Errorf("(Redis) one of the search options requires an command line argument")
	}

	// Check for incompatible options
	io := make([]string, 0, 3)	// Incompatible options
	for k, v := range map[string]bool{
		"deep": pc.QueryArgs.DeepSearch,
		"only-name": pc.QueryArgs.OnlyName,
		"only-tags": pc.QueryArgs.OnlyTags,
	} {
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
