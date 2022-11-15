package search

import (
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/cmd/dfi/internal/cfg"
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
	refsCSums := make(map[string]csData, len(refObjs))

	for id, fso := range refObjs {
		// Append checksums to query arguments
		qa.AddChecksums(fso.Checksum)
		// Make checksum<=>csData pair
		refsCSums[fso.Checksum] = csData{id: id, size: fso.Size}
	}

	// Clear search phrases due to them contain identifiers that should not be used in search
	qa.SetSearchPhrases(nil)

	// Run query to get duplicates. Append checksum field to return fields set,
	// to have ability to match found duplicates with provided references
	qr, err := dbc.Query(qa, []string{dbms.FieldID, dbms.FieldChecksum, dbms.FieldSize})
	if err != nil {
		rv.AddErr("cannot execute search query to find duplicates: %v", err)
	}

	// Create checksum-based object map - checksum is key, dupeInfo is value
	cdm := dupesMapByCSum(refsCSums, qr, rv)

	// Create resulted map with referred object<=>duplicates list pairs
	objDupes, nd := dupesMapById(refObjs, cdm)

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

func dupesMapByCSum(refsCSums map[string]csData, qr dbms.QueryResults, rv *types.CmdRV) map[string][]dupeInfo {
	dm := make(map[string][]dupeInfo, len(qr))

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
		if !ok {
			continue
		}

		// Check for extracted size differ than size of the referenced object
		if refsCSums[csum].size != size {
			// Such strange situation, it looks like
			// we found checksum collision, add warning and skip it
			rv.AddWarn("An object %s (%s) was found that has the same checksum as reference object %s, " +
						"but size of this object (%d) is different that the referenced object (%d) - " +
						"looks like a hash function collision! So, we skip this object",
						id, objKey, refsCSums[csum].id, size, refsCSums[csum].size)
			// Skip it
			continue
		}

		// Push identifier to duplicates map
		dm[csum] = append(dm[csum], dupeInfo{id: id, objKey: objKey})
	}

	return dm
}

// dupesMapById creates resulted map with referred object<=>duplicates list pairs
func dupesMapById(refObjs map[string]*types.FSObject, dm map[string][]dupeInfo) (map[string][]dupeInfo, int64) {
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

	return objDupes, nd
}
