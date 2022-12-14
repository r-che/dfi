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
	EvRemovePrefix
)
func (et eventType) String() string {
	switch et {
	case EvCreate: return "Create"
	case EvWrite: return "Write"
	case EvRemove: return "Remove"
	case EvRemovePrefix: return "RemovePrefix"
	default:
		panic(fmt.Sprintf("Unhandled filesystem event type %d", et))
	}
}

type FSEvent struct {
	Type		eventType
}
