//go:build dbi_mongo
package cfg

import (
	"fmt"
	"strings"
)

func (pc *progConfig) prepareDBMS() error {
	// Check for incompatible options
	io := make([]string, 0, 3)	// Incompatible options
	for k, v := range map[string]bool{
		"dupes": pc.SearchDupes,
		"only-name": pc.QA.OnlyName,
		"only-tags": pc.QA.OnlyTags,
		"only-descr": pc.QA.OnlyDescr,
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
