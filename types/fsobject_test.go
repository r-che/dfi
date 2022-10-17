package types

import (
	"testing"
)

func Test_eventTypeString(t *testing.T) {
	tests := []struct{
		eType	eventType
		want	string
	} {
		{ EvCreate, "Create" },
		{ EvWrite, "Write" },
		{ EvRemove, "Remove" },
	}

	for _, test := range tests {
		if str := test.eType.String(); str != test.want {
			t.Errorf("event %d returned string %q, want - %q", test.eType, str, test.want)
		}
	}
}

func Test_eventTypeUnsupported(t *testing.T) {
	et := eventType(-1)
	var str string
	defer func() {
		if p := recover(); p == nil {
			t.Errorf("the invalid event type %#v returned %q by String() method, but must panic", et, str)
		}
	}()

	str = et.String()
}

func TestObjKeyString(t *testing.T) {
	ok := ObjKey{Host: "test-host", Path: "/path/to/some/data"}

	want := "test-host:/path/to/some/data"
	if okStr := ok.String(); okStr != want {
		t.Errorf("ObjKey method String returned %q, want - %q", okStr, want)
	}
}

func TestObjKeyLess(t *testing.T) {
	// ok1 < ok2 by host
	ok1 := ObjKey{Host: "test-host1", Path: "/path/to/some/data"}
	ok2 := ObjKey{Host: "test-host2", Path: "/path/to/some/data"}
	if !ok1.Less(ok2) {
		t.Errorf("Object key %#v is not lesser than %#v, but must", ok1, ok2)
	}

	// ok3 < ok4 by path
	ok3 := ObjKey{Host: "test-host3", Path: "/path/to/dir1"}
	ok4 := ObjKey{Host: "test-host3", Path: "/path/to/dir2"}
	if !ok3.Less(ok4) {
		t.Errorf("Object key %#v is not lesser than %#v, but must", ok3, ok4)
	}

	// ok5 == ok6
	ok5 := ObjKey{Host: "test-host4", Path: "/path/to/data"}
	ok6 := ObjKey{Host: "test-host4", Path: "/path/to/data"}
	if ok5.Less(ok6) || ok6.Less(ok5) {
		t.Errorf("Object key %#v is not equal to %#v, but must", ok5, ok6)
	}

	// ok7 > ok8 by host
	ok7 := ObjKey{Host: "test-host6", Path: "/path/to/data"}
	ok8 := ObjKey{Host: "test-host5", Path: "/path/to/data"}
	if ok7.Less(ok8) {
		t.Errorf("Object key %#v not lesser than %#v, but must", ok7, ok8)
	}

	// ok9 > ok10 by host path
	ok9 := ObjKey{Host: "test-host7", Path: "/path/to/dir2"}
	ok10 := ObjKey{Host: "test-host7", Path: "/path/to/dir1"}
	if ok9.Less(ok10) {
		t.Errorf("Object key %#v not lesser than %#v, but must", ok9, ok10)
	}
}
