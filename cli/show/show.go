package show

import (
	"fmt"
	"strings"
	"time"
	"strconv"
	"sort"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cli/internal/cfg"

)

func Do(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	if c.UseTags {
		return showTags(dbc)
	}

	// Get identifiers list from configuration
	ids := c.CmdArgs

	// Function's return value
	rv := types.NewCmdRV()

	// Get objects list from DB
	objs, err := dbc.GetObjects(ids, dbms.UVObjFields())
	if err != nil {
		// Append the error
		rv.AddErr("cannot get requested objects from DB: %v", err)

		if len(objs) == 0 {
			// Nothing to do in such a case, just return error
			return rv
		}
	}
	rv.AddFound(int64(len(objs)))

	// Check loaded object for correctness
	for objKey, fields := range objs {
		id, ok := fields[dbms.FieldID]
		if !ok {
			rv.AddWarn("Skip loaded object %q without identifier field %q", objKey, dbms.FieldID)
			// Delete the invalid entry
			delete(objs, objKey)

			continue
		}

		// Check that ID has a correct type
		if _, ok := id.(string); !ok {
			rv.AddWarn("Skip loaded object %q due to identifier field %q is not a string: %v", objKey, dbms.FieldID, id)
			// Delete the invalid entry
			delete(objs, objKey)
		}
	}

	// Check for not-found objects
	if len(objs) != len(ids) {
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
	}

	// Get AII for objects
	aiis, err := dbc.GetAIIs(ids, dbms.UVAIIFields())
	if err != nil {
		// Append error to return value
		rv.AddErr("cannot get additional information about some objects: %v", err)
	}

	// Print all found objects
	showObjs(ids, objs, aiis)

	// Return results
	return rv
}

func showTags(dbc dbms.Client) *types.CmdRV {
	// Get configuration
	c := cfg.Config()
	// Get tags list specified from command line
	tags := tools.NewStrSet(c.CmdArgs...)

	// Function's return value
	rv := types.NewCmdRV()

	// Get list of objects with tags
	ids, err := dbc.GetAIIIds([]string{dbms.AIIFieldTags})
	if err != nil {
		return rv.AddErr("cannot get list of objects with tags: %w", err)
	}

	// Get tags by identifiers
	qr, err := dbc.GetAIIs(ids, []string{dbms.AIIFieldTags})
	if err != nil {
		return rv.AddErr("cannot get objects with tags: %w", err)
	}

	//
	// Collect all tags from selected objects
	//

	// Map with tag<=>times
	tt := map[string]int{}

	// Check that tags were not provided by command line
	if tags.Empty() {
		// Add all tags
		for _, aii := range qr {
			for _, tag := range aii.Tags {
				tt[tag]++
			}
		}
	} else {
		// Add only specified tags
		for _, aii := range qr {
			for _, tag := range aii.Tags {
				if (*tags)[tag] {
					tt[tag]++
				}
			}
		}
		// If not quiet mode - need to add any not found but requested tags with zero values
		if !c.Quiet {
			for _, tag := range tags.List() {
				if _, ok := tt[tag]; !ok {
					tt[tag] = 0
				}
			}
		}
	}

	// Make a list tags, sorted by number of occurrences
	keys := make([]string, 0, len(tt))
	for k := range tt {
		keys = append(keys, k)
	}
	// Sort in REVERSE order - the higher values should be the first
	sort.Slice(keys, func(i, j int) bool {
		// Compare by number of occurrences
		if cond := tt[keys[i]] > tt[keys[j]]; cond {
			return true
		} else if tt[keys[i]] == tt[keys[j]] {
			// Need to compare by keys in alphabetical order
			return keys[i] < keys[j]
		}

		return false
	})

	// Produce output
	if c.Quiet {
		// Quiet mode - only tags with non-zero number of occurrences
		for _, k := range keys {
			fmt.Println(k)
		}
	} else {
		// Normal mode - print: number_occurrences\ttag\n
		for _, k := range keys {
			fmt.Printf("%d\t%s\n", tt[k], k)
		}
	}

	return rv.AddFound(int64(len(tt)))
}

func showObjs(ids []string, objs dbms.QueryResults, aiis dbms.QueryResultsAII) {
	// Get configuration
	c := cfg.Config()

	// Print objects in the same order as input identifiers list
	ikm := make(map[string]types.ObjKey, len(ids))	// id->key map
	for objKey, fields := range objs {
		ikm[fields[dbms.FieldID].(string)] = objKey
	}

	switch {
	// JSON output
	case c.JSONOut:
		showJSONOutput(ids, ikm, objs, aiis)
	// One-line output
	case c.OneLine:
		for _, id := range ids {
			showObjOL(ikm[id], objs[ikm[id]], aiis[id])
		}
	// Default output
	default:
		for _, id := range ids {
			showObj(ikm[id], objs[ikm[id]], aiis[id])
		}
	}
}

func showJSONOutput(ids []string, ikm map[string]types.ObjKey, objs dbms.QueryResults, aiis dbms.QueryResultsAII) {
	// Get configuration
	c := cfg.Config()

	// Check for empty result
	if len(objs) == 0 {
		fmt.Println(`[]`)
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
	fmt.Print(`[` + nl)

	// Print items
	for i, id := range ids {
		objKey := ikm[id]

		// Buffer to collect values before output
		res := make([]string, 0, len(dbms.UVObjFields()) + len(dbms.UVAIIFields()) + 2 /* host + path */)

		res = append(res,
			fmt.Sprintf(`"host":%q`, objKey.Host),
			fmt.Sprintf(`"path":%q`, objKey.Path),
		)

		for _, field := range dbms.UVObjFields() {
			val, ok := objs[objKey][field]
			// If value empty/not set
			if !ok {
				res = append(res, `""`)
				continue
			}

			if s, ok := val.(string); ok {
				res = append(res, fmt.Sprintf(`%q:%q`, field, s))
			} else {
				res = append(res, fmt.Sprintf(`%q:%q`, field, fmt.Sprintf("%#v", val)))
			}
		}

		//
		// Add AII values
		//

		if aiis[id] != nil {
			// Tags
			tags := make([]string, 0, len(aiis[id].Tags))
			for _, tag := range aiis[id].Tags {
				tags = append(tags, fmt.Sprintf("%q", tag))
			}
			res = append(res, fmt.Sprintf("%q:[%s]", dbms.AIIFieldTags, strings.Join(tags, ",")))

			// Description
			res = append(res, fmt.Sprintf(`%q:%q`, dbms.AIIFieldDescr, aiis[id].Descr))
		}

		fmt.Print(ind + `{` + nl +							// opening brace
			ind + ind +										// indentation before first key
			strings.Join(res, `,` + nl + ind + ind) + nl +	// join with comma, new line and indentation all key-value pairs
			ind + `}`,										// closing brace
		)

		if i != len(ids) - 1 {
			fmt.Print(`,`)
		}
		fmt.Print(nl)
	}

	// End of JSON container
	fmt.Print(`]` + "\n")
}

func showObjOL(objKey types.ObjKey, fields map[string]any, aii *dbms.AIIArgs) {
	res := make([]string, 0, len(dbms.UVObjFields()) + len(dbms.UVAIIFields()) + 2 /* host + path */)
	res = append(res,
		fmt.Sprintf("host:%q", objKey.Host),
		fmt.Sprintf("path:%q", objKey.Path),
	)
	for _, field := range dbms.UVObjFields() {
		val, ok := fields[field]
		// If value empty/not set
		if !ok {
			res = append(res, `""`)
			continue
		}

		if s, ok := val.(string); ok {
			res = append(res, fmt.Sprintf("%s:%q", field, s))
		} else {
			res = append(res, fmt.Sprintf("%s:%q", field, fmt.Sprintf("%#v", val)))
		}
	}

	//
	// Add AII values
	//

	if aii != nil {
		// Tags
		res = append(res, fmt.Sprintf("%s:%q", dbms.AIIFieldTags, strings.Join(aii.Tags, ",")))

		// Description
		res = append(res, fmt.Sprintf("%s:%q", dbms.AIIFieldDescr, aii.Descr))
	} else {
		// Empty values of tags and description
		res = append(res, fmt.Sprintf(`%s:""`, dbms.AIIFieldTags))
		res = append(res, fmt.Sprintf(`%s:""`, dbms.AIIFieldDescr))
	}

	fmt.Println(strings.Join(res, " "))
}

func showObj(objKey types.ObjKey, fields map[string]any, aii *dbms.AIIArgs) {
	// Object header
	fmt.Printf(">>> [ID: %s]\n", fields[dbms.FieldID])

	// Common object's information
	fmt.Printf("Host:      %s\n", objKey.Host)
	fmt.Printf("Path:      %s\n", objKey.Path)

	// Is real path was set
	if rp := fields[dbms.FieldRPath]; rp != "" {
		fmt.Printf("Real path: %s\n", rp)
	}

	fmt.Printf("Type:      %s\n", fields[dbms.FieldType])
	fmt.Printf("Size:      %s\n", fields[dbms.FieldSize])

	// Check for mtime field found
	if mtime, ok := fields[dbms.FieldMTime]; ok {
		// Convert interface{} to string
		mtimeStr, ok := mtime.(string)
		if !ok {
			// Set mtimeStr to invalid value
			mtimeStr = fmt.Sprintf("non-string %v", mtime)
		}

		// Convert string Unix timestamp to integer value
		ts, err := strconv.ParseInt(mtimeStr, 10, 64)
		if err == nil {
			fmt.Printf("MTime:     %s (Unix: %s)\n",
				time.Unix(ts, 0).Format("2006-01-02 15:04:05 MST"),
				mtimeStr)
		} else {
			fmt.Printf("MTime:    INVALID VALUE %q - %v\n", mtimeStr, err)
		}
	}

	// Is checksum was set
	if csum := fields[dbms.FieldChecksum]; csum != "" {
		fmt.Printf("Checksum:  %s\n", csum)
	}

	// Print additional information if exists
	if aii != nil {
		if tags := strings.Join(aii.Tags, ","); tags != "" {
			fmt.Printf("Tags:      %s\n", tags)
		}
		if aii.Descr != "" {
			// Prepend each description line by double space
			fmt.Printf("Description:\n%s\n", `  ` + strings.ReplaceAll(aii.Descr, "\n", "\n  "))
		}
	}

	fmt.Println()
}
