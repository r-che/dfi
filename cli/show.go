package main

import (
	"fmt"
	"strings"
	"time"
	"strconv"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"

)

var showObjFields = []string{
	dbi.FieldID,
	dbi.FieldRPath,
	dbi.FieldType,
	dbi.FieldSize,
	dbi.FieldMTime,
	dbi.FieldChecksum,
}

var showAIIFields = []string{
	dbi.AIIFieldTags,
	dbi.AIIFieldDescr,
}

func doShow(dbc dbi.DBClient) *types.CmdRV {
	// Get configuration
	c := cfg.Config()

	// Get identifiers list from configuration
	ids := c.CmdArgs

	// Function's return value
	rv := types.NewCmdRV()

	// Get objects list from DB
	objs, err := dbc.GetObjects(ids, showObjFields)
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
		id, ok := fields[dbi.FieldID]
		if !ok {
			rv.AddWarn("Skip loaded object %q without identifier field %q", objKey, dbi.FieldID)
			// Delete the invalid entry
			delete(objs, objKey)

			continue
		}

		// Check that ID has a correct type
		if _, ok := id.(string); !ok {
			rv.AddWarn("Skip loaded object %q due to identifier field %q is not a string: %v", objKey, dbi.FieldID, id)
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
				if fields[dbi.FieldID] == ids[i] {
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
	aiis, err := dbc.GetAIIs(ids, showAIIFields)
	if err != nil {
		// Append error to return value
		rv.AddErr("cannot get additional information about some objects: %v", err)
	}

	// Print all found objects
	printObjs(ids, objs, aiis, c.OneLine)

	// Return results
	return rv
}

func printObjs(ids []string, objs dbi.QueryResults, aiis map[string]map[string]string, oneLine bool) {
	// Print objects in the same order as input identifiers list
	ikm := make(map[string]dbi.QRKey, len(ids))	// id->key map
	for objKey, fields := range objs {
		ikm[fields[dbi.FieldID].(string)] = objKey
	}

	for _, id := range ids {
		// Extract corresponding object key
		objKey := ikm[id]
		// Extract object fields
		fields := objs[objKey]
		if oneLine {
			printObjOL(objKey, fields, aiis[id])
		} else {
			printObj(objKey, fields, aiis[id])
		}

	}
}

func printObjOL(objKey dbi.QRKey, fields map[string]any, aii map[string]string) {
	res := make([]string, 0, len(showObjFields) + len(showAIIFields) + 2 /* host + path */)
	res = append(res,
		fmt.Sprintf("host:%q", objKey.Host),
		fmt.Sprintf("path:%q", objKey.Path),
	)
	for _, field := range showObjFields {
		val, ok := fields[field]
		// If value empty/not set
		if !ok {
			res = append(res, `""`)
			continue
		}

		if s, ok := val.(string); ok {
			res = append(res, fmt.Sprintf("%s:%q", field, s))
		} else {
			res = append(res, fmt.Sprintf("%q\n", fmt.Sprintf("%#v", val)))
		}
	}

	for _, field := range showAIIFields {
		val, ok := aii[field]
		// If value empty/not set
		if !ok {
			res = append(res, `""`)
			continue
		}

		res = append(res, fmt.Sprintf("%s:%q", field, val))
	}

	fmt.Println(strings.Join(res, " "))
}

func printObj(objKey dbi.QRKey, fields map[string]any, aii map[string]string) {
	// Object header
	fmt.Printf(">>> [ID: %s]\n", fields[dbi.FieldID])

	// Common object's information
	fmt.Printf("Host:      %s\n", objKey.Host)
	fmt.Printf("Path:      %s\n", objKey.Path)

	// Is real path was set
	if rp := fields[dbi.FieldRPath]; rp != "" {
		fmt.Printf("Real path: %s\n", rp)
	}

	fmt.Printf("Type:      %s\n", fields[dbi.FieldType])
	fmt.Printf("Size:      %s\n", fields[dbi.FieldSize])

	// Check for mtime field found
	if mtime, ok := fields[dbi.FieldMTime]; ok {
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
	if csum := fields[dbi.FieldChecksum]; csum != "" {
		fmt.Printf("Checksum:  %s\n", csum)
	}

	// Print additional information if exists
	if len(aii) != 0 {
		if tags := aii[dbi.AIIFieldTags]; tags != "" {
			fmt.Printf("Tags:      %s\n", aii[dbi.AIIFieldTags])
		}
		if descr := aii[dbi.AIIFieldDescr]; descr != "" {
			// Prepend each description line by double space
			fmt.Printf("Description:\n%s\n", `  ` + strings.ReplaceAll(descr, "\n", "\n  "))
		}
	}

	fmt.Println()
}
