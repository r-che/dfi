package dbms

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/r-che/dfi/types"

	"github.com/r-che/testing/clone"
)

func TestClone(t *testing.T) {
	sv := clone.NewStructVerifier(
		// Creator function
		func() any { return NewQueryArgs() },
		// Cloner function
		func(x any) any {
			qa, ok := x.(*QueryArgs)
			if ! ok {
				panic(fmt.Sprintf("unsupported type to clone: got - %T, want - *QueryArgs", x))
			}
			return qa.Clone()
		},
	).AddSetters(
		func() clone.Setter {
			return func(v reflect.Value) any {
				switch v.Interface().(type) {
				case types.SearchFlags:
					return types.SearchFlags{}
				case types.CommonFlags:
					return types.CommonFlags{}
				}
				return nil
			}
		},
	).AddChangers(
		func(v reflect.Value) bool {
			if _, ok := v.Interface().(types.SearchFlags); ok {
				v.Set(reflect.ValueOf(types.SearchFlags{true, true, true, true, true, true}))
			} else if _, ok  := v.Interface().(types.CommonFlags); ok {
				v.Set(reflect.ValueOf(types.CommonFlags{true, true}))
			} else {
				return false
			}
			return true
		},
	)

	err := sv.Verify()

	if err != nil {
		t.Errorf("verification of cloning QueryArgs failed: %v", err)
	}
}

func TestSetSearchPhrases(t *testing.T) {
	tests := []struct { input, want []string } {
		{	// 0
			input: []string{"Hello", "world", "!"},
			want: []string{"Hello", "world", "!"},
		},
		{	// 1
			input: []string{"  Hello ", " world", " !  "},
			want: []string{"Hello", "world", "!"},
		},
		{	// 2
			input: []string{"  Hello world", " !  "},
			want: []string{"Hello world", "!"},
		},
		{	// 3
			input: []string{" Hello ", "world!"},
			want: []string{"Hello", "world!"},
		},
		{	// 4
			input: []string{"Hello world!"},
			want: []string{"Hello world!"},
		},
		{	// 5
			input: []string{},
			want: []string{},
		},
	}

	for i, test := range tests {
		qa := NewQueryArgs().
			SetSearchPhrases(test.input)

		if !reflect.DeepEqual(qa.SP, test.want) {
			t.Errorf("[%d] want - %#v, got - %#v", i, test.want, qa.SP)
		}
	}
}

func TestParseMtimes(t *testing.T) {
	// The timestamp used as the reference time in the Go time module
	const refTS = int64(1136239445)	// "01/02 03:04:05PM '06 -0700"

	//
	// Test setting of set timestamps, use all available formats
	//

	// Slice to collect prepared values of times
	tsFormats := []string{}
	for i, layout := range TSFormats() {
		tsFormats = append(tsFormats, time.Unix(refTS + int64(i), 0).Format(layout))
	}
	// Slice to collect TSes created by standard time package functions as references
	refTSs := make([]int64, 0, len(tsFormats))
	for i, layout := range TSFormats() {
		ts, err := time.Parse(layout, tsFormats[i])
		if err != nil {
			// This must NOT happen
			panic(`time.Parse() cannot parse date "` + tsFormats[i] + `" created by Time.Format()`)
		}

		// Append Unix TS to references
		refTSs = append(refTSs, ts.Unix())

		// Update tsFormat value - escape comma, because it is the set delimiter
		tsFormats[i] = strings.ReplaceAll(tsFormats[i], ",", `\,`)
	}

	qa := NewQueryArgs()
	if err := qa.ParseMtimes(strings.Join(tsFormats, ",")); err != nil {
		t.Errorf("setting of the set mtimes failed: %v", err)
		t.FailNow()
	}

	// TNeed to compare value of each parsed mtime with reference TS
	for i, ts := range qa.MtimeSet {
		if ts != refTSs[i] {
			t.Errorf("[%d] incorrect value parsed from %q: want - %d, got - %d",
				i, tsFormats[i], refTS, ts)
		}
	}
}
