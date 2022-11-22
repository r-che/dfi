package show

import (
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
)

// removeInvalids checks loaded object for correctness and remove invalid objects from objs
func removeInvalids(objs dbms.QueryResults, rv *types.CmdRV) {
	for objKey, fields := range objs {
		id, ok := fields[dbms.FieldID]
		if !ok {
			rv.AddWarn("Skip loaded object %q without identifier field %q", objKey, dbms.FieldID)
			// Delete the invalid entry
			delete(objs, objKey)
		} else
		// Check that ID has a correct type
		if _, ok := id.(string); !ok {
			rv.AddWarn("Skip loaded object %q due to identifier field %q is not a string: %v", objKey, dbms.FieldID, id)
			// Delete the invalid entry
			delete(objs, objKey)
		}
	}
}

// removeNotFound removes from ids identifiers that not present in the objs
func removeNotFound(objs dbms.QueryResults, ids []string, rv *types.CmdRV) []string {
	if len(objs) == len(ids) {
		// All identifiers were found, return as is
		return ids
	}

	// Some objects were not found
	checkId:
	for i := 0; i < len(ids); {
		for _, fields := range objs {
			// Check that extracted identifier is equal requested identifier
			if fields[dbms.FieldID] == ids[i] {
				// Ok, requested identifier found, check next
				i++
				continue checkId
			}
		}
		// rqId was not found in loaded identifiers
		rv.AddWarn("Object with ID %q was not found or is incorrect", ids[i])
		// Remove not-found ID from ids slice
		ids = append(ids[:i], ids[i+1:]...)
	}

	return ids
}
