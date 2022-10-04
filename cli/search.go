package main

import (
	"fmt"
	"sort"
	"strings"

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

	rv := types.NewCmdRV()

	if c.SearchDupes {
		// TODO
		// Need to load information about objects for which need to search duplicates
		_, err := loadDupesRefs(dbc, rv)
		_=err
	}
	qr, err := dbc.Query(c.QueryArgs, rqFields)
	if err != nil {
		rv.AddErr("cannot execute DB query to show requested objects: %v", err)
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

func loadDupesRefs(dbc dbms.Client, rv *types.CmdRV) (any, error) {
	// Get configuration
	c := cfg.Config()

	// Create query arguments with identifiers
	qa := dbms.NewQueryArgs().AppendIds(c.CmdArgs)	// Search phrases used as list of identifiers

	// Run query to get information about the objects
	qr, err := dbc.Query(qa, []string{dbms.FieldID, dbms.FieldChecksum, dbms.FieldSize})
	if err != nil {
		return nil, err
	}

	// Make map of requested IDs mapped to corresponding checksum
	ids := make(map[string]string, len(qa.Ids))
	for _, id := range qa.Ids {
		ids[id] = ""
	}

	// Assign checksums
	for objKey, fields := range qr {
		// Extract identifier
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
			rv.AddWarn("Skip object %q with ID %q - this ID was not requested! Skip it", objKey, id)
			continue
		}

		csumVal, ok := fields[dbms.FieldChecksum]
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q without checksum field %q", objKey, dbms.FieldChecksum)
			continue
		}

		// Convert checksum to string representation
		csum, ok := csumVal.(string)
		if !ok {
			// Skip incorrect object
			rv.AddWarn("Skip invalid object %q with non-string checksum value: %#v", objKey, idVal)
			continue
		}

		// Assign checksum to corresponding ID
		ids[id] = csum
	}

	// Check for idenfifiers without checksum value
	nxIds := make([]string, 0, len(ids))
	for id, csum := range ids {
		if csum == "" {
			nxIds = append(nxIds, id)
		}
	}
	if len(nxIds) != 0 {
		return nil, fmt.Errorf("(RedisCli:searchDupes) requested objects do not exist: %s", strings.Join(nxIds, ", "))
	}

	return nil, nil
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
