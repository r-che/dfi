package fswatcher

import "fmt"

//
// Filesystem events
//
type eventType int
const (
	EvCreate	= eventType(iota)
	EvWrite
	EvRemove
)
func (et eventType) String() string {
	switch et {
	case EvCreate: return "Create"
	case EvWrite: return "Write"
	case EvRemove: return "Remove"
	default:
		panic(fmt.Sprintf("Unhandled filesystem event type %d", et))
	}
}

type FSEvent struct {
	Type		eventType
}
