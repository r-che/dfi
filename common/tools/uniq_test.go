package tools

import (
	"testing"
	"reflect"
)

func TestStrSet(t *testing.T) {
	tests := []struct {
		init	[]string
		add		[]string
		empty	bool
		lVal	[]string
		sVal	string
	} {
		{	// Init empty
			empty:	true,
			add:	[]string{"val1", "val2", "val3"},
			lVal:	[]string{"val1", "val2", "val3"},
			sVal:	"(val1, val2, val3)",
		},
		{	// Init non-empty
			init:	[]string{"val30", "val10", "val20"},
			add:	[]string{"val05", "val15", "val25"},
			lVal:	[]string{"val05", "val10", "val15", "val20", "val25", "val30"},
			sVal:	"(val05, val10, val15, val20, val25, val30)",
		},
		{	// Init empty, add duplicates
			empty:	true,
			add:	[]string{"val3", "val2", "val3", "val1", "val2", "val1", "val0"},
			lVal:	[]string{"val0", "val1", "val2", "val3"},
			sVal:	"(val0, val1, val2, val3)",
		},
		{	// Init non-empty, add duplicates
			init:	[]string{"val1", "val2", "val3"},
			add:	[]string{"val3", "val2", "val1", "val0"},
			lVal:	[]string{"val0", "val1", "val2", "val3"},
			sVal:	"(val0, val1, val2, val3)",
		},
	}

	for testN, test := range tests {
		// Init new set
		s := NewStrSet(test.init...)

		// Test for empty
		if s.Empty() != test.empty {
			t.Errorf("[%d] method Empty returns %t, want - %t", testN, s.Empty(), test.empty)
			// Go to next test
			continue
		}

		// Test for adding values
		s.Add(test.add...)

		// Test for produced list
		if l := s.List(); !reflect.DeepEqual(l, test.lVal) {
			t.Errorf("[%d] method List returned %#v, want - %#v", testN, l, test.lVal)
			// Go to next test
			continue
		}

		// Test for produced string
		if str := s.String(); str != test.sVal {
			t.Errorf("[%d] method String returned %q, want - %q", testN, str, test.sVal)
			// Go to next test
			continue
		}
	}
}
