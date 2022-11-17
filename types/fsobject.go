package types

const (
	// Stubs to fill checksum field on special cases
	CsTooLarge = `<FILE TOO LARGE>`
	CsErrorStub = `<FAIL TO CALCULATE CHECKSUM>`
)

//
// Filesystem object
//
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

// Supported object types
const (
	// XXX Do not forget to update ObjTypes() when you change this list
	ObjRegular		=	"reg"
	ObjDirectory	=	"dir"
	ObjSymlink		=	"sym"
)

func ObjTypes() []string {
	return []string {
		ObjRegular,
		ObjDirectory,
		ObjSymlink,
	}
}

//
// Filesystem object key
//
type ObjKey struct {
	Host string
	Path string
}
func (k ObjKey) String() string {
	return k.Host + `:` + k.Path
}
func (k ObjKey) Less(other ObjKey) bool {
	if k.Host < other.Host {
		return true
	}
	if k.Host == other.Host {
		return k.Path < other.Path
	}
	return false
}
