package types

import (
	"fmt"
	"strings"
)

// Command return value
type CmdRV struct {
	changed	int64
	found	int64
	errs	[]string
	wrns	[]string
}

func NewCmdRV() *CmdRV {
	return &CmdRV{}
}

func (rv *CmdRV) AddErr(args ...any) *CmdRV {
	appendMsg(&rv.errs, args...)
	return rv
}

func (rv *CmdRV) AddWarn(args ...any) *CmdRV {
	appendMsg(&rv.wrns, args...)
	return rv
}

func (rv *CmdRV) AddChanged(v int64) *CmdRV {
	rv.changed += v
	return rv
}

func (rv *CmdRV) AddFound(v int64) *CmdRV {
	rv.found += v
	return rv
}

func (rv *CmdRV) ErrsJoin(sep string) error {
	if rv.errs == nil {
		return nil
	}

	return fmt.Errorf("%s", strings.Join(rv.errs, sep))
}

func (rv *CmdRV) Errs() []string {
	return rv.errs
}

func (rv *CmdRV) Warns() []string {
	return rv.wrns
}

func (rv *CmdRV) Changed() int64 {
	return rv.changed
}

func (rv *CmdRV) Found() int64 {
	return rv.found
}

func (rv *CmdRV) OK() bool {
	return len(rv.errs) == 0 && len(rv.wrns) == 0
}

/*
 * Auxiliary functions
 */

func appendMsg(list *[]string, args ...any) {
	if *list == nil {
		*list = []string{}
	}
	switch len(args) {
		case 0:
			// Do nothing
		case 1:
			(*list) = append(*list, fmt.Sprintf("%v", args[0]))
		default:
			if format, ok := args[0].(string); ok {
				(*list) = append(*list, fmt.Sprintf(format, args[1:]...))
			} else {
				// Invalid value provided as format
				(*list) = append(*list, fmt.Sprintf("!s(%v) %v", args[0], args[1:]))
			}
	}
}
