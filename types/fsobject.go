package types

import "fmt"

const (
	// Stubs to fill checksum field on special cases
	CsTooLarge = `<FILE TOO LARGE>`
	CsErrorStub = `<FAIL TO CALCULATE CHECKSUM>`
)

type FSObject struct {
	// XXX Do not forget to update FSObjectFieldsNum on changing number of fields in this structure
	Name		string
	FPath		string	// Found path
	RPath		string	// Real object path
	Type		string	// Regular file, directory, symbolic link, etc...
	Size		int64
	MTime		int64
	Checksum	string
}
const FSObjectFieldsNum = 7

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

type objType string
const (
	ObjRegular		=	"reg"
	ObjDirectory	=	"dir"
	ObjSymlink		=	"sym"
)

// Values of this map represents key-value pairs with requested objects properties
type ObjKey struct {
	Host string
	Path string
}
func (k ObjKey) String() string {
	return k.Host + `:` + k.Path
}
