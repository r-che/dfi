package dbms

import (
	"fmt"
	"testing"
	"reflect"

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
