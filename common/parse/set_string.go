package parse

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/common/tools"
)

func SetString(fp *[]string, setName, vals string, allowed ...string) error {
	// Make list of unique values from vals
	*fp = tools.UniqStrings(strings.Split(vals, ","))

	// If no allowed values provided
	if len(allowed) == 0 {
		// Check only for empty values
		for _, v := range *fp {
			if v == "" {
				return fmt.Errorf("empty %s value in set-string %q", setName, vals)
			}
		}
		// OK
		return nil
	}

	parseItem:
	for _, val := range *fp {
		for _, av := range allowed {
			if val == av {
				continue parseItem
			}
		}

		return fmt.Errorf("incorrect %s value %q in set-string %q", setName, val, vals)
	}

	// OK
	return nil
}
