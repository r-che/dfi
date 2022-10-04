package main

import (
	"fmt"
	"sort"
	"strings"
	"strconv"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cli/internal/cfg"
)

//
// Dupes search related types
//

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

func doSearch(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	if c.SearchDupes {
		return searchDupes(dbc, c.QueryArgs)
	}

	// Set of requested fields
	rqFields := []string{}
	if c.NeedID() {
		// Add object identifier field to requested list
		rqFields = append(rqFields, dbms.FieldID)
	}

	rv := types.NewCmdRV()

	qr, err := dbc.Query(c.QueryArgs, rqFields)
	if err != nil {
		rv.AddErr("cannot execute search query: %v", err)
	}

	// Print results
	if c.HostGroups {
		// Print results grouped by hosts
		printResHG(qr)
	} else {
		// Print single-line sorted output
		printResSingle(qr)
	}

	// OK
	return rv.AddFound(int64(len(qr)))
}

func searchDupes(dbc dbms.Client, qa *dbms.QueryArgs) *types.CmdRV {
	rv := types.NewCmdRV()

	// Need to load information about referred objects which will use to found duplicates
	refObjs, err := loadDupesRefs(dbc, rv)
	if err != nil {
		return rv.AddErr(err)
	}

	// Map contains the correspondence between checksum<=>referred object
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
	// to have ability to match found duplicates with provided refrences
	qr, err := dbc.Query(qa, []string{dbms.FieldID, dbms.FieldChecksum, dbms.FieldSize})
	if err != nil {
		rv.AddErr("cannot execute search query to find duplicates: %v", err)
	}

	// Create a map of duplicates
	dm := map[string][]dupeInfo{}

	// Dupes counter
	var nd int64
	for objKey, fields := range qr {
		// TODO Need to create unified extraction function
		//
		// Extract identifier
		//
		idVal, ok := fields[dbms.FieldID]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q without identifier field %q", objKey, dbms.FieldID)
			continue
		}

		// Convert identifier to string representation
		id, ok := idVal.(string)
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q with non-string identifier value: %#v", objKey, idVal)
			continue
		}

		// Lookup identifier in refObjs
		if _, ok := refObjs[id]; ok {
			// This is one of the referred objects, skip
			continue
		}

		//
		// Extract checksum
		//
		csumVal, ok := fields[dbms.FieldChecksum]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) without checksum field %q", id, objKey, dbms.FieldChecksum)
			continue
		}

		// Convert checksum to string representation
		csum, ok := csumVal.(string)
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) with non-string checksum value: %#v", id, objKey, csumVal)
			continue
		}

		//
		// Extract size
		//
		sizeVal, ok := fields[dbms.FieldSize]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) without size field %q", id, objKey, dbms.FieldSize)
			continue
		}

		// Convert size to string representation
		sizeStr, ok := sizeVal.(string)
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) with non-string size value: %#v", id, objKey, sizeVal)
			continue
		}

		// Convert string size to integer
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) with incorrect value of size field %q: %q", id, objKey, dbms.FieldSize, sizeStr)
		}

		//
		// Check for extracted size differ than size of the refrenced object
		//
		if cr[csum].size != size {
			// Such strange situation, it looks like
			// we found checksum collision, add warning and skip it
			rv.AddWarn("An object %s (%s) was found that has the same checksum as referred object %s, " +
						"but size of this object (%d) is different that the referenced object (%d) - " +
						"looks like a hash function collision! So, we skip this object",
						id, objKey, cr[csum].id, size, cr[csum].size)
			// Skip it
			continue
		}

		// Push identifier to duplicates map
		dm[csum] = append(dm[csum], dupeInfo{id: id, objKey: objKey})
		// Increment dupes counter
		nd++
	}

	// Make output
	printDupes(refObjs, dm)

	return rv.AddFound(nd)
}

func printDupes(refObjs map[string]*types.FSObject, dm map[string][]dupeInfo) {
	// Get configuration
	c := cfg.Config()
	// Get list of requested identifiers to print results in the same order
	ids := c.CmdArgs

	// Remove identifiers for which do not have duplicates/were removed as unsuitable to search
	for i := 0; i < len(ids); {
		fso, ok := refObjs[ids[i]]
		if !ok {
			// Remove from identifiers list
			ids = append(ids[:i], ids[i+1:]...)
			continue
		}

		// Check that we have duplicates for this object
		if _, ok := dm[fso.Checksum]; !ok {
			// Remove from identifiers list
			ids = append(ids[:i], ids[i+1:]...)
			continue
		}

		i++
	}

	if c.OneLine {
		// Produce oneline output
		for _, id := range ids {
			// Get duplicates for id
			dupes := dm[refObjs[id].Checksum]
			// Sort by object keys
			sort.Slice(dupes, func(i, j int) bool {
				return dupes[i].objKey.Less(dupes[j].objKey)
			})

			// Print refrence identifier
			fmt.Print(id)
			// Print all dupes of id
			for _, did := range dupes {
				fmt.Printf(" %s", did)
			}
			// Print new line
			fmt.Println()
		}
	} else {
		// Produce normal output
		for _, id := range ids {
			// Get referred object
			ro := refObjs[id]
			// Get duplicates for id
			dupes := dm[ro.Checksum]
			// Sort by object keys
			sort.Slice(dupes, func(i, j int) bool {
				return dupes[i].objKey.Less(dupes[j].objKey)
			})

			fmt.Printf("%s %s (%d):\n", id,
				ro.FPath,	// XXX loadDupesRefs() kept object key in this field
				len(dupes))
			// Print all dupes of id
			for _, did := range dupes {
				fmt.Printf("  %s\n", did)
			}
		}
	}
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
		//
		// Extract identifier
		//
		idVal, ok := fields[dbms.FieldID]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q without identifier field %q", objKey, dbms.FieldID)
			continue
		}

		// Convert identifier to string representation
		id, ok := idVal.(string)
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q with non-string identifier value: %#v", objKey, idVal)
			continue
		}

		// Check that this object really was requested
		if _, ok := objRefs[id]; !ok {
			// Skip strange object
			rv.AddWarn("Skip object %s (%s) - this ID was not requested! Skip it", id, objKey)
			continue
		}

		//
		// Extract type field
		//
		oType, ok := fields[dbms.FieldType]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) without type field %q", id, objKey, dbms.FieldType)
			continue
		}
		if oType != types.ObjRegular {
			// Skip incorrect object
			rv.AddWarn("Object %s (%s) is not a regular file (%s) - cannot search duplicates for it", id, objKey, oType)
			// Remove it from requested identifiers map
			delete(objRefs, id)

			continue
		}

		//
		// Extract checksum
		//
		csumVal, ok := fields[dbms.FieldChecksum]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) without checksum field %q", id, objKey, dbms.FieldChecksum)
			continue
		}

		// Convert checksum to string representation
		csum, ok := csumVal.(string)
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %s (%s) with non-string checksum value: %#v", id, objKey, csumVal)
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

		//
		// Extract size
		//
		sizeVal, ok := fields[dbms.FieldSize]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q without size field %q", objKey, dbms.FieldSize)
			continue
		}

		// Convert size to string representation
		sizeStr, ok := sizeVal.(string)
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q with non-string size value: %#v", objKey, sizeVal)
			continue
		}

		// Convert string size to integer
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q with incorrect value of size field %q: %q", objKey, dbms.FieldSize, sizeStr)
		}

		// Assign collected data to map as FSObject
		objRefs[id] = &types.FSObject{
			Checksum:	csum,
			Size:		size,
			FPath:		objKey.String(),	// XXX Use FPath field to pass refrence object key to printing function
		}
	}

	// Check for idenfifiers without checksum value
	nxIds := make([]string, 0, len(objRefs))
	for id, v := range objRefs {
		if v == nil {
			nxIds = append(nxIds, id)
		}
	}
	if len(nxIds) != 0 {
		return nil, fmt.Errorf("requested objects do not exist: %s", strings.Join(nxIds, ", "))
	}

	return objRefs, nil
}

func printResHG(qr dbms.QueryResults) {
	// Get configuration
	c := cfg.Config()

	// Make lists grouped by hosts
	hg := map[string][]string{}

	for k := range qr {
		// Check existence of value for the host from k
		if _, ok := hg[k.Host]; ok {
			// Just add path
			hg[k.Host] = append(hg[k.Host], k.Path)
		} else {
			// Initiate new slice for paths
			list := []string{k.Path}
			// Set value for host
			hg[k.Host] = list
		}
	}

	// Sort hosts
	hosts := make([]string, 0, len(hg))
	for h := range hg {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)

	// Print paths grouped by hosts
	for _, host := range hosts {
		// Make list of sorted paths for this host
		paths := hg[host]
		sort.Strings(paths)

		fmt.Printf("%s(%d):\n", host, len(paths))

		switch {
		case c.ShowOnlyIds:
			for _, path := range paths {
				fmt.Printf("  %v\n", qr[types.ObjKey{host, path}][dbms.FieldID])
			}
		case c.ShowID:
			for _, path := range paths {
				fmt.Printf("  %v %s\n", qr[types.ObjKey{host, path}][dbms.FieldID], path)
			}
		default:
			for _, path := range paths {
				fmt.Printf("  %s\n", path)
			}
		}
	}
}

func printResSingle(qr dbms.QueryResults) {
	// Get configuration
	c := cfg.Config()

	// Make sorted list of query result keys
	qrKeys := make([]types.ObjKey, 0, len(qr))
	for k := range qr {
		qrKeys = append(qrKeys, k)
	}
	sort.Slice(qrKeys, func(i, j int) bool {
		if qrKeys[i].Host < qrKeys[j].Host {
			return true
		}
		if qrKeys[i].Host == qrKeys[j].Host {
			return qrKeys[i].Path < qrKeys[j].Path
		}
		return false
	})

	switch {
	case c.ShowOnlyIds:
		for _, k := range qrKeys {
			fmt.Printf("%v\n", qr[k][dbms.FieldID])
		}
	case c.ShowID:
		for _, k := range qrKeys {
			fmt.Printf("%v %s:%s\n", qr[k][dbms.FieldID], k.Host, k.Path)
		}
	default:
		for _, k := range qrKeys {
			fmt.Printf("%s:%s\n", k.Host, k.Path)
		}
	}
}
