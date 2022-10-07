package search

import (
	"fmt"
	"sort"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func Do(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	if c.SearchDupes {
		return searchDupes(dbc, c.QA)
	}

	// Set of requested fields
	rqFields := []string{}
	if c.NeedID() {
		// Add object identifier field to requested list
		rqFields = append(rqFields, dbms.FieldID)
	}

	rv := types.NewCmdRV()

	if c.QA.IsAIIFields() {
		// Search for identifiers of objects that have filled requested AII fields
		ids, err := dbc.GetAIIIds(c.QA.AIIFields)
		if err != nil {
			return rv.AddErr("cannot search for objects with filled fields %v: %v", c.QA.AIIFields, err)
		}

		c.QA.AddIds(ids...)
	}

	if c.QA.UseAII() {
		// Search by AII fields
		ids, err := dbc.QueryAIIIds(c.QA)
		if err != nil {
			return rv.AddErr("cannot search by additional information objects fields: %v", err)
		}

		// Check for only AII should be used in search
		if c.QA.OnlyAII() {
			// If no identifiers by AII were found
			if len(ids) == 0 {
				// Than nothing to search, return empty result
				return rv
			}

			// Clear search phrases to avoid using them in the next search
			c.QA.SetSearchPhrases(nil)
		}

		c.QA.AddIds(ids...)
	}

	qr, err := dbc.Query(c.QA, rqFields)
	if err != nil {
		rv.AddErr("cannot execute search query: %v", err)
	}

	// Print results
	switch {
	// JSON results
	case c.JSONOut:
		printJSON(qr)
	// Results grouped by hosts
	case c.HostGroups:
		printResHG(qr)
	// Print single-line sorted output
	default:
		printResSingle(qr)
	}

	// OK
	return rv.AddFound(int64(len(qr)))
}

func printJSON(qr dbms.QueryResults) {
	// Get configuration
	c := cfg.Config()

	if len(qr) == 0 {
		fmt.Println(`[]`)
		return
	}

	// Make sorted list of query result keys
	objKeys := make([]types.ObjKey, 0, len(qr))
	for k := range qr {
		objKeys = append(objKeys, k)
	}
	sort.Slice(objKeys, func(i, j int) bool {
		return objKeys[i].Less(objKeys[j])
	})

	// New line if required
	nl := "\n"
	// Indent if required
	ind := "    "
	if c.OneLine {
		// Clear new line character and indentation
		nl = ""
		ind = ""
	}

	var printer func(types.ObjKey, map[string]any)

	switch {
	case c.ShowOnlyIds:
		printer = func(k types.ObjKey, r map[string]any) {
			fmt.Printf(ind + `%q`, r[dbms.FieldID])
		}
	case c.ShowID:
		printer = func(k types.ObjKey, r map[string]any) {
			fmt.Printf(
				ind + `{` + nl +
				ind + ind + `"%s": %q,` + nl +
				ind + ind + `"objKey": %q` + nl +
				ind + `}`, dbms.FieldID, r[dbms.FieldID], k)
		}
	default:
		printer = func(k types.ObjKey, r map[string]any) {
			fmt.Printf(ind + `%q`, k)
		}
	}

	// Start of JSON container
	fmt.Print(`[` + nl)

	// Print items
	for i, key := range objKeys {
		printer(key, qr[key])
		if i != len(objKeys) - 1 {
			fmt.Print(`,`)
		}
		fmt.Print(nl)
	}

	// End of JSON container
	fmt.Print(`]` + "\n")
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
	objKeys := make([]types.ObjKey, 0, len(qr))
	for k := range qr {
		objKeys = append(objKeys, k)
	}
	sort.Slice(objKeys, func(i, j int) bool {
		return objKeys[i].Less(objKeys[j])
	})

	switch {
	case c.ShowOnlyIds:
		for _, k := range objKeys {
			fmt.Printf("%v\n", qr[k][dbms.FieldID])
		}
	case c.ShowID:
		for _, k := range objKeys {
			fmt.Printf("%v %s:%s\n", qr[k][dbms.FieldID], k.Host, k.Path)
		}
	default:
		for _, k := range objKeys {
			fmt.Printf("%s:%s\n", k.Host, k.Path)
		}
	}
}