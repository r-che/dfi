package parse

import (
	"reflect"
	"testing"
)

func TestStringsSet(t *testing.T) {
	tests := []struct {
		name	string
		vals	string
		allowed	[]string
		want	[]string
		errStr	string
	} {
		// Positive tests
		{	// Normal set, no errors, no allowed values
			name:		"normal-set",
			vals:		"val1,val2,val3,val4,val5",
			want:		[]string{"val1", "val2", "val3", "val4", "val5"},
		},
		{	// Normal set, no errors, with allowed values
			name:		"normal-set,with-allowed",
			vals:		"val2,val3,val4",
			allowed:	[]string{"val1", "val2", "val3", "val4", "val5"},
			want:		[]string{"val2", "val3", "val4"},
		},
		{	// Normal set, no errors, empty value
			name:		"normal-set,with-allowed-empty",
			vals:		"val2,,val4,val5",
			allowed:	[]string{"val1", "val2", "", "val4", "val5"},
			want:		[]string{"", "val2", "val4", "val5"},
		},

		// Negative tests
		{	// Set with disallowed value
			name:		"incorrect-set,disallowed-value",
			vals:		"val1,val2,VAL3,val4,val5",
			allowed:	[]string{"val1", "val2", "val3", "val4", "val5"},
			want:		[]string{"val1", "val2", "val3", "val4", "val5"},
			errStr:		`incorrect incorrect-set,disallowed-value value "VAL3" in the input string: "val1,val2,VAL3,val4,val5"`,
		},
		{	// Set with empty values, no allowed values
			name:		"incorrect-set-with-empty,no-allowed",
			vals:		"val1,val2,,val4,,val5",
			want:		[]string{"val1", "val2", "val4", "val5"},
			errStr:		`empty incorrect-set-with-empty,no-allowed value in the input string: "val1,val2,,val4,,val5"`,
		},
		{	// Another set with empty values, no allowed values
			name:		"incorrect-set-with-empty,with-allowed",
			vals:		",val1,val2,val3,val4,val5,",
			want:		[]string{"val1", "val2", "val3", "val4", "val5"},
			errStr:		`empty incorrect-set-with-empty,with-allowed value in the input string: ",val1,val2,val3,val4,val5,"`,
		},
	}

	for testN, test := range tests {
		// Slice to save result
		res := []string{}
		// Run function
		err := StringsSet(&res, test.name, test.vals, test.allowed...)
		if err == nil {
			// No error, need to check for negative test
			if test.errStr != "" {
				// In fact - test must fail
				t.Errorf("[%d] case with arguments %#v must fail, but it did not", testN, test)
				// Go to next test
				continue
			}
			// Ok, test did not fail, continue checks
		} else {
			// Check for errStr is not set
			if test.errStr == "" {
				// This is a real error - test should be This is a real error - test should be succeed
				t.Errorf("[%d] case with arguments %#v failed: %v", testN, test, err)
				// Go to next test
				continue
			}

			// Compare errors
			if err.Error() != test.errStr {
				// Unexpected error
				t.Errorf("[%d] case with arguments %#v failed with unexpected error %q - want error %q",
					testN, test, err, test.errStr)
			}
			// Go to next test - there is no need to check result
			continue
		}

		// Test for produced list
		if !reflect.DeepEqual(res, test.want) {
			t.Errorf("[%d] case returns result %#v, want - %#v", testN, res, test.want)
			// Go to next test
			continue
		}

		// Test passed
	}
}
