package types

import "fmt"

type FSObject struct {
	Name		string
	FPath		string	// Found path
	RPath		string	// Real object path
	Type		string	// Regular file, directory, symbolic link, etc...
	LinksNum	uint	// Number of hardlinks
	Size		uint
	CheckSum	[]byte
}

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
