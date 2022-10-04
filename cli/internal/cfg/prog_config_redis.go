//go:build dbi_redis
package cfg

import (
	"fmt"
	"strings"
)

func (pc *progConfig) prepareDBMS() error {
	// Check for incompatible options
	io := make([]string, 0, 3)	// Incompatible options
	for k, v := range map[string]bool{
		"deep": pc.QueryArgs.DeepSearch,
		"dupes": pc.SearchDupes,
		"only-name": pc.QueryArgs.OnlyName,
		"only-tags": pc.QueryArgs.OnlyTags,
		"only-descr": pc.QueryArgs.OnlyDescr,
	} {
		if v {
			io = append(io, `--` + k)
		}
	}

	if len(io) < 2 {
		// OK
		return nil
	}

	return fmt.Errorf("search options are incompatible: %s", strings.Join(io, " "))
}
