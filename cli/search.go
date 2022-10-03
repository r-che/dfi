package main

import (
	"fmt"
	"sort"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func doSearch(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	// Set of requested fields
	rqFields := []string{}
	if c.PrintID || c.OnlyIds {
		// Add object identifier field to requested list
		rqFields = append(rqFields, dbms.FieldID)
	}

	rv := types.NewCmdRV()

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
		case c.OnlyIds:
			for _, path := range paths {
				fmt.Printf("  %v\n", qr[types.ObjKey{host, path}][dbms.FieldID])
			}
		case c.PrintID:
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
	case c.OnlyIds:
		for _, k := range qrKeys {
			fmt.Printf("%v\n", qr[k][dbms.FieldID])
		}
	case c.PrintID:
		for _, k := range qrKeys {
			fmt.Printf("%v %s:%s\n", qr[k][dbms.FieldID], k.Host, k.Path)
		}
	default:
		for _, k := range qrKeys {
			fmt.Printf("%s:%s\n", k.Host, k.Path)
		}
	}
}
