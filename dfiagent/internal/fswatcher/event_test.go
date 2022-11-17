package fswatcher

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
		{ EvRemovePrefix, "RemovePrefix" },
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
