package types

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func makeSyncMap() (*SyncMap, map[string]any) {
	sm := NewSyncMap()

	vals := map[string]any {
		"string value": "Any string data",
		"float 64 bit size": 3.141_592_653,
		"boolean value": true,
		"slice int value": []int{1, 2, 3, 4},
		"empty interface": interface{}(nil),
		"nil value": nil,
		"struct value": struct {a, b bool; c string}{false, true, "c member value"},
	}

	for k, v := range vals {
		sm.Set(k, v)
	}

	return sm, vals
}

//
// Positive tests
//
func TestSyncMapLen(t *testing.T) {
	sm, vals := makeSyncMap()

	// Test Len
	if l := sm.Len(); l != len(vals) {
		t.Errorf("invalid map length of map, want - %d, got %d", len(vals), l)
		t.FailNow()
	}
}

func TestSyncMapGet(t *testing.T) {
	sm, vals := makeSyncMap()

	for k, v := range vals {
		gv, ok := sm.Get(k)
		if !ok {
			t.Errorf("key %q is not found in the map", k)
			t.FailNow()
		}

		// Compare values
		if !reflect.DeepEqual(v, gv) {
			t.Errorf("key %q has wrong value, want - %#v, got - %#v", k, v, gv)
			continue
		}
	}
}

func TestSyncMapVal(t *testing.T) {
	sm, vals := makeSyncMap()

	for k, v := range vals {
		if gv := sm.Val(k); !reflect.DeepEqual(v, gv) {
			t.Errorf("key %q has wrong value, want - %#v, got - %#v", k, v, gv)
			continue
		}
	}
}

func TestSyncMapDel(t *testing.T) {
	sm, vals := makeSyncMap()

	// Test Del
	for k := range vals {
		// Get current length
		l := sm.Len()

		// Delete key
		sm.Del(k)

		// Check for key was really deleted
		if _, ok := sm.Get(k); ok {
			t.Errorf("key %q must NOT be found after deletion, but it was found, map content: %#v", k, sm.m)
			t.FailNow()
		}

		// Check for correct length
		if nl := sm.Len(); nl != l-1 {
			t.Errorf("invalid length after deletion, want - %d, got - %d, map content: %#v", l-1, nl, sm.m)
			t.FailNow()
		}
	}
}

func TestSyncMapApply(t *testing.T) {
	// Initiate map
	sm := NewSyncMap()
	vals := map[string]any {
		"one":		int(1),
		"two":		int(2),
		"three":	int(3),
	}
	for k, v := range vals {
		sm.Set(k, v)
	}

	// Test Apply
	sm.Apply(func(_ string, v any) any {
		return v.(int) * 2
	})

	// Check results
	for k, v := range vals {
		vm := sm.Val(k).(int)
		if va := v.(int) * 2; va != vm {
			t.Errorf("key %q has wrong value %d, want - %d", k, vm, va)
		}
	}
}

func TestSyncMapConcurrentApply(t *testing.T) {
	// Test map size
	size := 100
	// Init map with set of values
	sm := NewSyncMap()
	// Sorted keys
	keys := make([]string, 0, size)
	for i := 0; i < size; i++ {
		keys = append(keys, fmt.Sprintf("%02d", i))
		sm.Set(keys[len(keys)-1], 0)
	}


	ch := make(chan bool)

	for i := 0; i < size; i++ {
		// Start concurrent goroutine
		go func() {
			sm.Apply(func(_ string, v any) any { return v.(int) +1 })
			ch <- true
		}()
	}

	// Wait for goroutines
	for i := 0; i < size; i++ {
		<-ch
	}

	// Check for Apply result - all values must be == size
	for _, k := range keys {
		if v := sm.Val(k); v.(int) != size {
			t.Errorf("wrong value %v of key %q, want - %d", v, k, size)
		}
	}
}

func TestSyncMapConcurrentDel(t *testing.T) {
	// Test map size
	size := 100
	// Init map with set of values
	sm := NewSyncMap()
	// Sorted keys
	keys := make([]string, 0, size)
	for i := 0; i < size; i++ {
		keys = append(keys, fmt.Sprintf("%02d", i))
		sm.Set(keys[len(keys)-1], true)
	}

	ch := make(chan bool)

	for _, k := range keys {
		// Start concurrent goroutine
		go func(key string) {
			sm.Del(key)
			ch <- true
		}(k)
	}

	// Wait for goroutines
	for i := 0; i < size; i++ {
		<-ch
	}

	// Map should be empty
	if l := sm.Len(); l != 0 {
		t.Errorf("map is not empty (%d keys) after concurrent deletions: %#v", l, sm.m)
	}
}

//
// Negative tests
//

func TestSyncMapError(t *testing.T) {
	ref := "test error"
	err := &SyncMapError{errors.New(ref)}
	if ev := err.Error(); ev != ref {
		t.Errorf("SyncMapError.Error() returned %q, want - %q", ev, ref)
	}
}

func TestSyncMapValFail(t *testing.T) {
	// Create an empty map
	sm := NewSyncMap()

	// Using nested function to catch panic
	func() {
		defer func() {
			switch p := recover(); p.(type) {
			case nil:
				t.Errorf("the Val method must panic, but it did not")
			case *SyncMapError:
				// Expected panic type
			default:
				t.Errorf("unexpected panic with argument type %T (value: %v)", p, p)
			}
		}()
		// Try to get any from it
		sm.Val("any")
	}()
}
