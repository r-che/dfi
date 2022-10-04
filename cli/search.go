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

func doSearch(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	// Set of requested fields
	rqFields := []string{}
	if c.NeedID() {
		// Add object identifier field to requested list
		rqFields = append(rqFields, dbms.FieldID)
	}

	if c.SearchDupes {
		return searchDupes(dbc, c.QueryArgs, rqFields)
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

func searchDupes(dbc dbms.Client, qa *dbms.QueryArgs, rqFields []string) *types.CmdRV {
	rv := types.NewCmdRV()

	// Need to load information about reference objects which will use to found duplicates
	refObjs, err := loadDupesRefs(dbc, rv)
	if err != nil {
		return rv.AddErr(err)
	}

	// Append checksums to query arguments
	for _, fso := range refObjs {
		qa.AddChecksums(fso.Checksum)
	}

	// Clear search phrases due to them contain identifiers that should not be used in search
	qa.SetSearchPhrases(nil)

	// Append checksum field to return fields set, to have ability
	// to match found duplicates with provided refrences
	rqFields = append(rqFields, dbms.FieldChecksum)
	// Run query to get duplicates
	qr, err := dbc.Query(qa, rqFields)
	if err != nil {
		rv.AddErr("cannot execute search query to find duplicates: %v", err)
	}

	// Create a map of duplicates
	dm := map[string][]string{}

	// Dupes counter
	var nd int64
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

		// Lookup identifier in refObjs
		if _, ok := refObjs[id]; ok {
			// This is one of the referenced objects, skip
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

		// Push identifier to duplicates map
		dm[csum] = append(dm[csum], id)
		// Increment dupes counter
		nd++
	}

	// Make output
	printDupes(refObjs, dm)

	return rv.AddFound(nd)
}

func printDupes(refObjs map[string]*types.FSObject, dm map[string][]string) {
	// Get configuration
	c := cfg.Config()
	// Get list of requested identifiers to print results in the same order
	ids := c.CmdArgs

	if c.OneLine {
		// Produce oneline output
		for _, id := range ids {
			// Check for any duplicates of reference id were found
			dupes, ok := dm[refObjs[id].Checksum]
			if !ok {
				// Skip
				continue
			}

			// Print refrence identifier
			fmt.Print(id)
			// Sort identifiers
			sort.Strings(dupes)
			// Print all dupes of id
			for _, did := range dupes {
				fmt.Printf(" %s", did)
			}
			// Print new line
			fmt.Println()
		}
	} else {
		// TODO Produce normal output
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
	ids := make(map[string]*types.FSObject, len(qa.Ids))
	for _, id := range qa.Ids {
		ids[id] = nil
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
		if _, ok := ids[id]; !ok {
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
			delete(ids, id)

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
		ids[id] = &types.FSObject{
			Checksum:	csum,
			Size:		size,
		}
	}

	// Check for idenfifiers without checksum value
	nxIds := make([]string, 0, len(ids))
	for id, v := range ids {
		if v == nil {
			nxIds = append(nxIds, id)
		}
	}
	if len(nxIds) != 0 {
		return nil, fmt.Errorf("requested objects do not exist: %s", strings.Join(nxIds, ", "))
	}

	return ids, nil
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
