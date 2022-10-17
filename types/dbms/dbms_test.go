package dbms

import (
	"testing"
	"sort"
	"reflect"
)

func TestUVObjFields(t *testing.T) {
	want := []string {
		FieldID,
		FieldRPath,
		FieldType,
		FieldSize,
		FieldMTime,
		FieldChecksum,
	}
	// Sort it by real values
	sort.Strings(want)

	fields := UVObjFields()
	// Also sort, order should be the same
	sort.Strings(fields)

	if !reflect.DeepEqual(fields, want) {
		t.Errorf("set of user valuable fields is incorrect: got - %#v, want - %#v", fields, want)
	}
}

func TestUVAIIFields(t *testing.T) {
	want := []string{
		AIIFieldTags,
		AIIFieldDescr,
	}

	// Sort it by real values
	sort.Strings(want)

	fields := UVAIIFields()
	// Also sort, order should be the same
	sort.Strings(fields)

	if !reflect.DeepEqual(fields, want) {
		t.Errorf("set of AII user valuable fields is incorrect: got - %#v, want - %#v", fields, want)
	}
}

func TestDBOperatorString(t *testing.T) {
	tests := []struct{
		opType	DBOperator
		want	string
	} {
		{ Update, "Update" },
		{ Delete, "Delete" },
	}

	for _, test := range tests {
		if str := test.opType.String(); str != test.want {
			t.Errorf("DBOperator %d returned string %q, want - %q", test.opType, str, test.want)
		}
	}
}

func Test_DBOperatorUnsupported(t *testing.T) {
	op := DBOperator(-1)
	var str string
	defer func() {
		if p := recover(); p == nil {
			t.Errorf("the invalid DBOperator %#v returned %q by String() method, but must panic", op, str)
		}
	}()

	str = op.String()
}
