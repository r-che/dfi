package types

import (
	"fmt"
	"strings"
)

// Command return value
type CmdRV struct {
	Changed	int64
	errs	[]string
	Warns	[]string
}

func NewCmdRV() *CmdRV {
	return &CmdRV{}
}

func (rv *CmdRV) AddErr(args ...any) *CmdRV {
	if rv.errs == nil {
		rv.errs = []string{}
	}

	switch len(args) {
		case 0:
			// Do nothing
		case 1:
			rv.errs = append(rv.errs, fmt.Sprintf("%v", args[0]))
		default:
			if format, ok := args[0].(string); ok {
				rv.errs = append(rv.errs, fmt.Sprintf(format, args[1:]...))
			} else {
				// Invalid value provided as format
				rv.errs = append(rv.errs, fmt.Sprintf("!s(%v) %v", args[0], args[1:]))
			}
	}

	return rv
}

func (rv *CmdRV) AddChanged(v int64) *CmdRV {
	rv.Changed += v
	return rv
}

func (rv *CmdRV) ErrsJoin(sep string) error {
	if rv.errs == nil {
		return nil
	}

	return fmt.Errorf("%s", strings.Join(rv.errs, sep))
}
