package parse

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/common/tools"
)

// StringsSet splits comma-separated list of values vals to the slice, sorts 
// them, and saves to *fp. Returns an error if an empty value is found in the 
// result set or if the result item does not belong to allowed list. The setName 
// variable is used to specify the name of the set in returned error
func StringsSet(fp *[]string, setName, vals string, allowed ...string) error {
	// Make list of unique values from vals
	*fp = tools.NewStrSet(strings.Split(vals, ",")...).List()

	// If no allowed values provided
	if len(allowed) == 0 {
		// Check only for empty values
		for _, v := range *fp {
			if v == "" {
				return fmt.Errorf("empty %s value in the input string: %q", setName, vals)
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

		return fmt.Errorf("incorrect %s value %q in the input string: %q", setName, val, vals)
	}

	// OK
	return nil
}
