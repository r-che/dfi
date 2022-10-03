//go:build dbi_redis
package cfg

import (
	"fmt"
	"strings"
)

func (pc *progConfig) prepareDBMS() error {
	io := make([]string, 0, 3)	// Incompatible options
	for k, v := range map[string]bool{"deep": pc.deepSearch, "only-name": pc.onlyName, "only-tags": pc.onlyTags} {
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
