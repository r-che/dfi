package search

import (
	"fmt"
	"sort"
	"strings"
	"strconv"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cli/internal/cfg"
)

// Data corresponding to checksum of some object
type csData struct {
	id		string
	size	int64
}
type dupeInfo struct {
	id		string
	objKey	types.ObjKey
}
func (di dupeInfo) String() string {
	return di.id + ` ` + di.objKey.String()
}

func searchDupes(dbc dbms.Client, qa *dbms.QueryArgs) *types.CmdRV {
	rv := types.NewCmdRV()

	// Need to load information about reference objects which will use to found duplicates
	refObjs, err := loadDupesRefs(dbc, rv)
	if err != nil {
		return rv.AddErr(err)
	}

	// Check for we have any data to check
	if len(refObjs) == 0 {
		// No data, return now
		return rv
	}

	// Map contains the correspondence between checksum<=>reference object
	cr := make(map[string]csData, len(refObjs))

	for id, fso := range refObjs {
		// Append checksums to query arguments
		qa.AddChecksums(fso.Checksum)
		// Make checksum<=>csData pair
		cr[fso.Checksum] = csData{id: id, size: fso.Size}
	}

	// Clear search phrases due to them contain identifiers that should not be used in search
	qa.SetSearchPhrases(nil)

	// Run query to get duplicates. Append checksum field to return fields set,
	// to have ability to match found duplicates with provided references
	qr, err := dbc.Query(qa, []string{dbms.FieldID, dbms.FieldChecksum, dbms.FieldSize})
	if err != nil {
		rv.AddErr("cannot execute search query to find duplicates: %v", err)
	}

	// Create a map of duplicates
	dm := map[string][]dupeInfo{}

	for objKey, fields := range qr {
		// Extract identifier
		id, ok := extrFieldStr(objKey, fields, dbms.FieldID, rv)
		if !ok {
			continue
		}

		// Extract checksum
		csum, ok := extrFieldStr(objKey, fields, dbms.FieldChecksum, rv)
		if !ok {
			continue
		}

		// Extract size
		size, ok := extrFieldInt64(objKey, fields, dbms.FieldSize, rv)
		if err != nil {
			continue
		}

		// Check for extracted size differ than size of the referenced object
		if cr[csum].size != size {
			// Such strange situation, it looks like
			// we found checksum collision, add warning and skip it
			rv.AddWarn("An object %s (%s) was found that has the same checksum as reference object %s, " +
						"but size of this object (%d) is different that the referenced object (%d) - " +
						"looks like a hash function collision! So, we skip this object",
						id, objKey, cr[csum].id, size, cr[csum].size)
			// Skip it
			continue
		}

		// Push identifier to duplicates map
		dm[csum] = append(dm[csum], dupeInfo{id: id, objKey: objKey})
	}

	//
	// Create resulted map with refered object<=>duplicates list pairs
	//
	objDupes := make(map[string][]dupeInfo)
	// Dupes counter
	var nd int64

	for id, fso := range refObjs {
		// Go over all duplicates with checksum of the fso
		for _, di := range dm[fso.Checksum] {
			// Skip self
			if id == di.id {
				continue
			}

			// Append this object to list of dupes of object with id
			objDupes[id] = append(objDupes[id], di)
			// Increment dupes counter
			nd++
		}
	}

	// Make an output
	printDupes(refObjs, objDupes)

	return rv.AddFound(nd)
}

func loadDupesRefs(dbc dbms.Client, rv *types.CmdRV) (map[string]*types.FSObject, error) {
	// Get configuration
	c := cfg.Config()

	// Create query arguments with identifiers
	qa := dbms.NewQueryArgs().AddIds(c.CmdArgs...)	// Search phrases used as list of identifiers

	// Run query to get information about the objects
	qr, err := dbc.Query(qa, []string{dbms.FieldID, dbms.FieldType, dbms.FieldChecksum, dbms.FieldSize})
	if err != nil {
		return nil, err
	}

	// Make map of requested IDs mapped to corresponding checksum
	objRefs := make(map[string]*types.FSObject, len(qa.Ids))
	for _, id := range qa.Ids {
		objRefs[id] = nil
	}

	// Assign checksums
	for objKey, fields := range qr {
		// Extract identifier
		id, ok := extrFieldStr(objKey, fields, dbms.FieldID, rv)
		if !ok {
			continue
		}
		// Check that this object really was requested
		if _, ok := objRefs[id]; !ok {
			// Skip strange object
			rv.AddWarn("Skip object %s (%s) - this ID was not requested! Skip it", id, objKey)
			continue
		}

		// Extract type field
		oType, ok := extrFieldStr(objKey, fields, dbms.FieldType, rv)
		if !ok {
			continue
		}
		if oType != types.ObjRegular {
			// Skip incorrect object
			rv.AddWarn("Object %s (%s) is not a regular file (%s) - cannot search duplicates for it", id, objKey, oType)
			// Remove it from requested identifiers map
			delete(objRefs, id)

			continue
		}

		// Extract checksum
		csum, ok := extrFieldStr(objKey, fields, dbms.FieldChecksum, rv)
		if !ok {
			continue
		}
		// Check the value of checksum
		switch csum {
		case "":
			rv.AddWarn("Skip object %s (%s) with empty checksum field", id, objKey)
			continue
		case types.CsTooLarge:
			rv.AddWarn("Skip object %s (%s) - checksum is not set because the file is too large", id, objKey)
			continue
		case types.CsErrorStub:
			rv.AddWarn("Skip object %s (%s) - checksum is not set because an error occurred during calculation", id, objKey)
			continue
		}

		// Extract size
		size, ok := extrFieldInt64(objKey, fields, dbms.FieldSize, rv)
		if !ok {
			continue
		}

		// Assign collected data to map as FSObject
		objRefs[id] = &types.FSObject{
			Checksum:	csum,
			Size:		size,
			FPath:		objKey.String(),	// XXX Use FPath field to pass reference object key to printing function
		}
	}

	// Check for idenfifiers without checksum value
	nxIds := make([]string, 0, len(objRefs))
	for id, v := range objRefs {
		if v == nil {
			nxIds = append(nxIds, id)
			// Remove invalid identifier
			delete(objRefs, id)
		}
	}
	if len(nxIds) != 0 {
		rv.AddWarn("Requested object(s) do not exist or invalid: %s", strings.Join(nxIds, ", "))
	}

	return objRefs, nil
}

func extrFieldStr(objKey types.ObjKey, fields map[string]any, fn string, rv *types.CmdRV) (string, bool) {
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

func extrFieldInt64(objKey types.ObjKey, fields map[string]any, fn string, rv *types.CmdRV) (int64, bool) {
	// Extract raw field value
	fVal, err := extrFieldRaw(fields, fn)
	if err != nil {
		rv.AddWarn("Skip invalid object with key %s - %w", objKey, err)
		return 0, false
	}

	// Check for value type
	switch fVal.(type) {
	// int64 value
	case int64:
		// Ok, return as is
		return fVal.(int64), true

	// string value
	case string:
		// Convert string to integer
		intVal, err := strconv.ParseInt(fVal.(string), 10, 64)
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

func printDupes(refObjs map[string]*types.FSObject, dm map[string][]dupeInfo) {
	// Get configuration
	c := cfg.Config()
	// Get list of requested identifiers to print results in the same order
	ids := c.CmdArgs

	// Remove identifiers for which do not have duplicates/were removed as unsuitable to search
	for i := 0; i < len(ids); {
		_, ok := refObjs[ids[i]]
		if !ok {
			// Remove from identifiers list
			ids = append(ids[:i], ids[i+1:]...)
			continue
		}

		i++
	}

	switch {
	// JSON output
	case c.JSONOut:
		printJSONDupes(ids, refObjs, dm)
	// Single-lined output
	case c.OneLine:
		// Produce oneline output
		for _, id := range ids {
			// Get duplicates for id
			dupes := dm[id]
			// Sort by object keys
			sort.Slice(dupes, func(i, j int) bool {
				return dupes[i].objKey.Less(dupes[j].objKey)
			})

			// Print reference identifier
			fmt.Print(id)
			// Print all dupes of id
			for _, did := range dupes {
				fmt.Printf(" %s", did.id)
			}
			// Print new line
			fmt.Println()
		}
	// Normal verbose multiline output
	default:
		for i, id := range ids {
			// Get duplicates for this object
			dupes := dm[id]
			// Sort duplicates by object keys
			sort.Slice(dupes, func(i, j int) bool {
				return dupes[i].objKey.Less(dupes[j].objKey)
			})

			// Get key of reference object
			objKey := refObjs[id].FPath	// XXX loadDupesRefs() kept object key in this field

			// Is no duplicates were found
			if len(dupes) == 0 {
				fmt.Printf("%s %s: No duplicates were found\n", id, objKey)
			} else {
				// Print all dupes of reference object
				fmt.Printf("%s %s (%d):\n", id, objKey, len(dupes))
				for _, did := range dupes {
					fmt.Printf("  %s\n", did)
				}
			}

			if i != len(ids) - 1 {
				fmt.Print("---")
			}
			fmt.Println()
		}
	}
}

func printJSONDupes(ids []string, refObjs map[string]*types.FSObject, dm map[string][]dupeInfo) {
	// Get configuration
	c := cfg.Config()

	if len(ids) == 0 {
		fmt.Println(`{}`)
		return
	}

	// New line if required
	nl := "\n"
	// Indent if required
	ind := "    "
	if c.OneLine {
		// Clear new line character and indentation
		nl = ""
		ind = ""
	}

	// Start of JSON container
	fmt.Print(`{` + nl)

	// Print items
	for i, id := range ids {
		// Get duplicates for this object
		dupes := dm[id]
		// Sort duplicates by object keys
		sort.Slice(dupes, func(i, j int) bool {
			return dupes[i].objKey.Less(dupes[j].objKey)
		})

		// Is no duplicates were found
		if len(dupes) == 0 {
			// No duplicates found
			fmt.Printf(ind + `%q: {}`, id)
		} else {
			fmt.Printf(ind + `%q: {` + nl, id)
			// Print all dupes of reference object
			for j, dinf := range dupes {
				fmt.Printf(ind + ind + `%q: %q`, dinf.id, dinf.objKey)
				if j != len(dupes) - 1 {
					fmt.Print(`,`)
				}
				fmt.Print(nl)
			}
			fmt.Print(ind + `}`)
		}

		if i != len(ids) - 1 {
			fmt.Print(`,`)
		}
		fmt.Print(nl)
	}

	// End of JSON container
	fmt.Print(`}` + "\n")
}
