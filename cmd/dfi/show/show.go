package show

import (
	"fmt"
	"strings"
	"time"
	"strconv"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cmd/dfi/internal/cfg"

)

const (
	invalidMtimeValueFmt	= `<INVALID-MTIME-VALUE(%#v) - %v>`
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
	removeInvalids(objs, rv)

	// Remove not-found objects, keep order of identifiers in ids
	ids = removeNotFound(objs, ids, rv)

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
		res := make([]string, 0, len(dbms.UVObjFields()) + len(dbms.UVAIIFields()) + 1 /* host */ + 1 /* path */)

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

func showObjOL(objKey types.ObjKey, fields dbms.QRItem, aii *dbms.AIIArgs) {
	res := make([]string, 0, len(dbms.UVObjFields()) + len(dbms.UVAIIFields()) + 1 /* host */ + 1 /* path */)
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

func showObj(objKey types.ObjKey, fields dbms.QRItem, aii *dbms.AIIArgs) {
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
	fmt.Printf("Size:      %v\n", fields[dbms.FieldSize])

	// Print object modification time
	showObjMtime(fields)

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

func showObjMtime(fields dbms.QRItem) {
	// Check for mtime field is not found
	mtime, ok := fields[dbms.FieldMTime]
	if !ok {
		// Nothing to print
		return
	}

	var mtimeTS int64
	var err error

	// Type of mtime field is DBMS dependent
	switch mtime := mtime.(type) {
	case int64:
		// Assign value to mtimeTS as is - it is already Unix timestamp
		mtimeTS = mtime
	case string:
		// Convert string Unix timestamp to integer value
		mtimeTS, err = strconv.ParseInt(mtime, 10, 64)
	default:
		err = fmt.Errorf("unexpected mtime type %T", mtime)
	}


	var mtimeStr string	// human-readable representation
	if err == nil {
		mtimeStr = time.Unix(mtimeTS, 0).Format("2006-01-02 15:04:05 MST")
	} else {
		mtimeStr = fmt.Sprintf(invalidMtimeValueFmt, mtime, err)
		mtimeTS = -1
	}

	// Produce output
	fmt.Printf("MTime:     %s (Unix: %d)\n", mtimeStr, mtimeTS)
}
