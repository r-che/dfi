package main

import (
	"fmt"
	"sort"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func doSearch(dbc dbi.DBClient) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	// Set of requested fields
	rqFields := []string{}
	if c.PrintID() {
		// Add object identifier field to requested list
		rqFields = append(rqFields, dbi.FieldID)
	}

	rv := types.NewCmdRV()

	qr, err := dbc.Query(c.QueryArgs(), rqFields)
	if err != nil {
		rv.AddErr("cannot execute DB query to show requested objects: %v", err)
	}

	// Print results
	if c.HostGroups() {
		// Print results grouped by hosts
		printResHG(qr, c.PrintID())
	} else {
		// Print single-line sorted output
		printResSingle(qr, c.PrintID())
	}

	// OK
	return rv.AddFound(int64(len(qr)))
}

func printResHG(qr dbi.QueryResults, printID bool) {
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

		if printID {
			for _, path := range paths {
				fmt.Printf("  %v %s\n", qr[dbi.QRKey{host, path}][dbi.FieldID], path)
			}
		} else {
			for _, path := range paths {
				fmt.Printf("  %s\n", path)
			}
		}
	}
}

func printResSingle(qr dbi.QueryResults, printID bool) {
	// Make sorted list of query result keys
	qrKeys := make([]dbi.QRKey, 0, len(qr))
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

	if printID {
		for _, k := range qrKeys {
			fmt.Printf("%v %s:%s\n", qr[k][dbi.FieldID], k.Host, k.Path)
		}
	} else {
		for _, k := range qrKeys {
			fmt.Printf("%s:%s\n", k.Host, k.Path)
		}
	}
}
