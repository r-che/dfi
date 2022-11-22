package search

import (
	"fmt"
	"strconv"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
)

func extrFieldStr(objKey types.ObjKey, fields dbms.QRItem, fn string, rv *types.CmdRV) (string, bool) {
	fVal, err := extrFieldRaw(fields, fn)
	if err != nil {
		rv.AddWarn("Skip invalid object with key %s - %w", objKey, err)
		return "", false
	}

	strVal, ok := fVal.(string)
	if !ok {
		rv.AddWarn("Skip invalid object %q with non-string value of field %q: %#v", objKey, fn,  fVal)
		return "", false
	}

	return strVal, true
}

func extrFieldInt64(objKey types.ObjKey, fields dbms.QRItem, fn string, rv *types.CmdRV) (int64, bool) {
	// Extract raw field value
	fVal, err := extrFieldRaw(fields, fn)
	if err != nil {
		rv.AddWarn("Skip invalid object with key %s - %w", objKey, err)
		return 0, false
	}

	// Check for value type
	switch fVal := fVal.(type) {
	// int64 value
	case int64:
		// Ok, return as is
		return fVal, true

	// string value
	case string:
		// Convert string to integer
		intVal, err := strconv.ParseInt(fVal, 10, 64)
		if err != nil {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q with incorrect value of field %q: %v", objKey, dbms.FieldSize, fVal)
			return 0, false
		}

		return intVal, true

	// Unsupported type
	default:
		rv.AddWarn("Skip invalid object %q with unsupported type of value of the field %q: %#v", objKey, fn,  fVal)
	}

	return 0, false
}

func extrFieldRaw(fields dbms.QRItem, fn string) (any, error) {
	fVal, ok := fields[fn]
	if ok {
		return fVal, nil
	}

	//
	// Requested was not found
	//

	// Check that the field identifier was requested
	if fn == dbms.FieldID || fields[dbms.FieldID] == "" {
		// Return error without identifier
		return nil, fmt.Errorf("object does not contain field %q", fn)
	}

	// Return error with identifier
	return nil, fmt.Errorf("object (ID: %s) does not contain field %q", fields[dbms.FieldID], fn)
}
