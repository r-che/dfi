package types

import (
	"testing"
	"fmt"
	"reflect"
	"strings"
)

func TestCmdRVAdd(t *testing.T) {
	rv := NewCmdRV()

	var nEvents, wantFound, wantChanged int64
	nEvents = 5

	wantErrs := []string{}
	wantWrns := []string{}

	for i := int64(0); i < nEvents; i++ {
		// Add error
		rv.AddErr("Formatted error #%d: integer value - %d, string value %q, struct value: %#v",
			i, 234 + i, "some error string here", struct{a, b bool; c string}{true, false, "c error data"}).
			AddErr(fmt.Sprintf("Not formatted error #%d", i))
		// Push the same to expected errors
		wantErrs = append(wantErrs, fmt.Sprintf(
			"Formatted error #%d: integer value - %d, string value %q, struct value: %#v",
			i, 234 + i, "some error string here", struct{a, b bool; c string}{true, false, "c error data"}),
			fmt.Sprintf("Not formatted error #%d", i))

		// Add warning
		rv.AddWarn("Formatted warning #%d: integer value - %d, string value %q, struct value: %#v",
			i, 567 + i * 2, "some warning string here", struct{i int64; b bool; c string}{45 + i * 2, false, "c warning data"}).
			AddWarn(fmt.Sprintf("Not formatted warning #%d", i))
		// Push the same to expected errors
		wantWrns = append(wantWrns, fmt.Sprintf(
			"Formatted warning #%d: integer value - %d, string value %q, struct value: %#v",
			i, 567 + i * 2, "some warning string here", struct{i int64; b bool; c string}{45 + i * 2, false, "c warning data"}),
			fmt.Sprintf("Not formatted warning #%d", i))

		// Update found and changed counters
		rv.AddFound(i).AddChanged(i * 2)
		// Update expectations
		wantFound += i
		wantChanged += i * 2
	}

	// Check results
	if e := rv.Errs(); !reflect.DeepEqual(e, wantErrs) {
		t.Errorf("got errors %#v, want - %#v", e, wantErrs)
	}
	if w := rv.Warns(); !reflect.DeepEqual(w, wantWrns) {
		t.Errorf("got warnings %#v, want - %#v", w, wantWrns)
	}
	if f := rv.Found(); f != wantFound {
		t.Errorf("got found counter %d, want - %d", f, wantFound)
	}
	if c := rv.Changed(); c != wantChanged {
		t.Errorf("got changed counter %d, want - %d", c, wantChanged)
	}
}

func TestCmdRVAddInvalidFormat(t *testing.T) {
	rv := NewCmdRV().AddErr(1, "arg#1", "arg#2", "arg#3")
	err := rv.ErrsJoin(", ")

	want := "!s(1) [arg#1 arg#2 arg#3]"

	if err == nil {
		t.Errorf("CmdRV (%#v) returned nil error by ErrsJoin method, want - %q", rv, want)
		t.FailNow()
	}

	if err.Error() != want {
		t.Errorf("CmdRV (%#v) returned error %q, want - %q", rv, err, want)
	}
}

func TestCmdRVErrsJoin(t *testing.T) {
	rv := NewCmdRV()

	if err := rv.ErrsJoin(", "); err != nil {
		t.Errorf("empty CmdRV (%#v) returned error by ErrsJoin method - %v, want - nil", rv, err)
	}

	errs := []string{
		"error #0",
		"error #1",
		"error #2",
	}

	for _, e := range errs {
		rv.AddErr(e)
	}

	want := strings.Join(errs, ", ")
	if got := rv.ErrsJoin(", "); got.Error() != want {
		t.Errorf("got - %q, want - %q, source errors list: %#v, rv.Errs: %#v", got, want, errs, rv.Errs())
	}
}

func TestCmdRVOK(t *testing.T) {
	rv := NewCmdRV()

	// Should be OK
	rv.AddFound(1).AddChanged(2)
	if !rv.OK() {
		t.Errorf("CmdRV with no warnings/errors (%#v) - method OK returns false, want - true", rv)
		t.FailNow()
	}

	// Should NOT be OK
	rv.AddErr("error")
	if rv.OK() {
		t.Errorf("CmdRV with error (%#v) - method OK returns true, want - false", rv)
		t.FailNow()
	}

	// Reinit to test with warning
	rv = NewCmdRV().AddWarn("warn")
	if rv.OK() {
		t.Errorf("CmdRV with warning (%#v) - method OK returns true, want - false", rv)
		t.FailNow()
	}

	// Reinit to test warnings and erorrs
	rv = NewCmdRV().AddWarn("warn").AddErr("error")
	if rv.OK() {
		t.Errorf("CmdRV with warning and error (%#v) - method OK returns true, want - false", rv)
		t.FailNow()
	}
}
