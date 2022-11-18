package tools

import (
	"testing"
)

func TestTern(t *testing.T) {
	tests := []struct{
		cond	bool
		ifTrue	any
		ifFalse	any
	} {
		// Integer tests
		{	// Positive
			cond:		true,
			ifTrue:		-1,
			ifFalse:	-2,
		},
		{	// Negative
			cond:		false,
			ifTrue:		999,
			ifFalse:	777,
		},

		// Strings tests
		{	// Positive
			cond:		true,
			ifTrue:		"",
			ifFalse:	"\u0234\u0000",
		},
		{	// Negative
			cond:		false,
			ifTrue:		999,
			ifFalse:	"True lies",
		},
	}

	for testN, test := range tests {
		val := Tern(test.cond, test.ifTrue, test.ifFalse)

		switch test.cond {
		case true:
			if val != test.ifTrue {
				t.Errorf("[%d] got - %v, want - %v", testN, val, test.ifTrue)
			}
		case false:
			if val != test.ifFalse {
				t.Errorf("[%d] got - %v, want - %v", testN, val, test.ifFalse)
			}
		}
	}
}
