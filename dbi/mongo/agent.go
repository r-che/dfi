package mongo

import (
	"fmt"
	"strings"
	"regexp"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/dbi/common"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Duplicates of original fields found path and name with values transformed for tokenization
const (
	MongoFieldTFPath	=	"tfpath"
	MongoFieldTName		=	"tname"
)

//
// Agent client interface
//

func (mc *Client) UpdateObj(fso *types.FSObject) error {
	// Push object to update queue

	if mc.ReadOnly {
		log.W("(MongoCli:UpdateObj) R/O mode IS SET, Insert/Update of %q collection" +
				" will NOT be performed => %s\n", MongoObjsColl, mc.Cfg.CliHost + ":" + fso.FPath)
		// Increase the update counter and return no errors
		mc.updated++
		// OK
		return nil
	}
	log.D("(MongoCli:UpdateObj) Insert/Update of collection %q => %s\n", MongoObjsColl, mc.Cfg.CliHost + ":" + fso.FPath)

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoObjsColl)

	// Update/Insert object
	id := common.MakeID(mc.Cfg.CliHost, fso)
	fields := bson.D{
		// Using mongo-specific identifier field name instead of standard dbms.FieldID
		{MongoFieldID,			id},
		// Standard fields set
		{dbms.FieldHost,		mc.Cfg.CliHost},
		{dbms.FieldName,		fso.Name},
		{dbms.FieldFPath,		fso.FPath},
		{dbms.FieldRPath,		fso.RPath},
		{dbms.FieldType,		fso.Type},
		{dbms.FieldSize,		fso.Size},
		{dbms.FieldMTime,		fso.MTime},
		{dbms.FieldChecksum,	fso.Checksum},
	}

	// Validate fields
	if err := validateUtf8Values(fields); err != nil {
		return fmt.Errorf("(MongoCli:UpdateObj) invalid filesytem object with path %q: %w", fso.FPath, err)
	}

	//
	// Improve tokenization:
	// MongoDB tokenizer does not do tokenization by underscores, but  underscores
	// are often used instead of spaces in file system object names. To  improve
	// full-text search - add additional fields with values created from
	// original FPath and Name values by replacing underscores with spaces
	//

	if strings.Contains(fso.FPath, "_") {
		fields = append(fields, bson.E{MongoFieldTFPath, strings.ReplaceAll(fso.FPath, "_", " ")})
	}
	if strings.Contains(fso.Name, "_") {
		fields = append(fields, bson.E{MongoFieldTName, strings.ReplaceAll(fso.Name, "_", " ")})
	}

	//
	// Update item
	//
	res, err := coll.UpdateOne(mc.Ctx,
		bson.D{{MongoFieldID, id}},			// Update exactly this ID
		bson.D{{`$set`, fields}},
		options.Update().SetUpsert(true),	// do insert if no object with this ID was found
	)
	if err != nil {
		return fmt.Errorf("(MongoCli:UpdateObj) updateOne (id: %s, found path: %q) on %s.%s failed: %w",
				id, fso.FPath, coll.Database().Name(), coll.Name(), err)
	}

	if res.MatchedCount == 0 && res.UpsertedCount == 0 {
		return fmt.Errorf("(MongoCli:UpdateObj) updateOne (id: %s) on %s.%s returned success," +
			" but no documents were changed", id, coll.Database().Name(), coll.Name())
	}

	// Increase the update counter and return no errors
	mc.updated++

	// OK
	return nil
}

func (mc *Client) DeleteObj(fso *types.FSObject) error {
	mongoID := common.MakeID(mc.Cfg.CliHost, fso)

	if mc.ReadOnly {
		log.W("(MongoCli:DeleteObj) R/O mode IS SET, will not be performed: Delete => %s:%s (%s)\n",
			mc.Cfg.CliHost, fso.FPath, mongoID)
	} else {
		log.D("(MongoCli:DeleteObj) Delete (pending) => %s:%s (%s)\n",
			mc.Cfg.CliHost, fso.FPath, mongoID)
	}

	// XXX Append key to delete regardless of R/O mode because it will be skipped in the Commit() operation
	mc.toDelete = append(mc.toDelete, mongoID)

	// OK
	return nil
}

func (mc *Client) DeleteFPathPref(fso *types.FSObject) (int64, error) {
	// Create a filter to load identifiers of documents belonging to this host, prefixed with fso.FPath
	filter := NewFilter().Append(
		bson.E{dbms.FieldHost, mc.Cfg.CliHost},
		bson.E{dbms.FieldFPath, primitive.Regex{Pattern: regexp.QuoteMeta(fso.FPath)}},
	)

	// Collect identifiers that need to be deleted
	delIds := []string{}

	err := mc.loadFieldByFilter(MongoFieldID, filter,
	// Append found value of identifiers to the list of identifiers that need to be deleted
	func(value any) error {
		id, ok := value.(string)
		// Check for invalid type of value
		if !ok {
			return fmt.Errorf("(MongoCli:DeleteFPathPref:appender) type of the %q field is %T, want - string," +
				" value: %#v", MongoFieldID, value, value)
		}

		delIds = append(delIds, id)

		// OK
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("(MongoCli:DeleteFPathPref) cannot load identifiers of objects belong" +
			" to the host %q prefixed with %q: %w", mc.Cfg.CliHost, fso.FPath, err)
	}

	log.D("(MongoCli:DeleteFPathPref) %d objects with %q field prefixed with %q will be deleted",
		len(delIds), dbms.FieldFPath, fso.FPath)

	// XXX Append key to delete regardless of R/O mode because it will be skipped in the Commit() operation
	mc.toDelete = append(mc.toDelete, delIds...)

	// OK
	return int64(len(delIds)), nil
}

func (mc *Client) Commit() (int64, int64, error) {
	// Reset state on return
	defer func() {
		// Reset counters
		mc.updated = 0
		mc.deleted = 0
		// Reset lists of queued data
		mc.toDelete = nil
	}()

	// Create filter by identifiers
	filter := bson.D{{
		MongoFieldID, bson.D{{ `$in`, mc.toDelete }},
	}}

	// Check for keys to delete
	if nDel := len(mc.toDelete); nDel != 0 {
		log.D("(MongoCli:Commit) Need to delete %d keys", nDel)

		deleted, err := mc.performDelete(filter)
		if err != nil {
			return 0, deleted, fmt.Errorf("(MongoCli:Commit) delete failed: %w", err)
		}

		mc.deleted += deleted

		log.D("(MongoCli:Commit) Done deletion operation")
	}

	// XXX Use intermediate variables to avoid resetting return values by deferred function
	ru, rd := mc.updated, mc.deleted

	return ru, rd, nil
}

func (mc *Client) performDelete(filter bson.D) (int64, error) {
	if mc.ReadOnly {
		// Simulate deletion by filter
		return mc.deleteDryRun(filter)
	}

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoObjsColl)

	// Perform deletion
	res, err := coll.DeleteMany(mc.Ctx, filter)

	// Is operation successful?
	if err == nil {
		return res.DeletedCount, nil
	}

	// Operation is not successful, try to extract number of deleted documents
	deleted := int64(0)
	if res != nil {
		deleted = res.DeletedCount
	}

	return deleted, fmt.Errorf("delete from %s.%s failed: %w", coll.Database().Name(), coll.Name(), err)
}

func (mc *Client) deleteDryRun(filter bson.D) (int64, error) {
	// Only load identifiers for all object that queued to deletion
	nd := []string{}	// will not be deleted
	wd := []string{}	// would be deleted

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoObjsColl)

	cursor, err := coll.Find(mc.Ctx, filter, options.Find().SetReturnKey(true))
	if err != nil {
		// Unexpected error
		return 0, fmt.Errorf("(MongoCli:Commit:deleteDryRun) find on %s.%s for identifiers %v failed: %w",
			coll.Database().Name(), coll.Name(), mc.toDelete, err)
	}

	//
	// Make a list of results
	//
	defer func() {
		if err := cursor.Close(mc.Ctx); err != nil {
			log.E("(MongoCli:Commit:deleteDryRun) cannot close cursor: %v", err)
		}
	}()

	// Make a set from keys that should be deleted
	dset := tools.NewStrSet(mc.toDelete...)

	// Keep current termLong value to have ability to compare during long-term operations
	initTermLong := mc.TermLongVal
	// Make a list of results
	for cursor.Next(mc.Ctx) {
		// If value of the termLong was updated - need to terminate long-term operation
		if mc.TermLongVal != initTermLong {
			return int64(len(wd)), fmt.Errorf("(MongoCli:Commit:deleteDryRun) terminated")
		}

		// Item to get ID and found-path from the query result
		var item map[string]string
		// Try to decode next cursor value to the item
		if err := cursor.Decode(&item); err != nil {
			log.E("(MongoCli:Commit) R/O mode, loading suitable to deletion object - cannot decode cursor item: %w", err)
			// All identifers will NOT be deleted
			nd = append(nd, mc.toDelete...)
			// Break cursor loop
			break
		}

		// Extract ID field
		id, ok := item[MongoFieldID]
		if !ok {
			log.E("(MongoCli:Commit:deleteDryRun) Cannot convert object identifier to string, skip: %#v", item)
			continue
		}

		// Check for membership
		if dset.Includes(id) {
			// Ok, this item will be deleted as expected
			wd = append(wd, id)
			dset.Del(id)
		} else {
			// Something strange - unexpected ID would be deleted
			log.E("(MongoCli:Commit) Delete (R/O mode) unexpected object would be deleted - id: %s, expected list: %v",
				id, dset.List())
		}
	}

	// All identifiers from dset - would NOT be deleted
	nd = append(nd, dset.List()...)

	// Update deleted counter by number of selected keys that would be deleted
	mc.deleted += int64(len(wd))

	// Check for keys that would be deleted
	if len(wd) != 0 {
		// Print warning message about these keys
		log.W("(MongoCli:Commit) %d key(s) should be deleted but would NOT because R/O mode: %v",
			len(wd), strings.Join(wd, ", "))
	}

	// Check for keys that would not be deleted
	if len(nd) != 0 {
		// Print warning
		log.W("(MongoCli:Commit) R/O mode - DEL could NOT delete %d keys" +
			" because not exist or other errors: %v", len(nd), strings.Join(nd, ", "))
	}

	return int64(len(wd)), nil
}

func (mc *Client) LoadHostPaths(match dbms.MatchStrFunc) ([]string, error) {
	// Output list of paths belong to the host
	hostPaths := []string{}

	log.D("(MongoCli:LoadHostPaths) Scanning %s.%s for objects belonging to the host %q ...",
		mc.Cfg.ID, MongoObjsColl, mc.Cfg.CliHost)

	filter := NewFilter().SetExpr(bson.D{{ dbms.FieldHost, mc.Cfg.CliHost }})

	err := mc.loadFieldByFilter(dbms.FieldFPath, filter,
	// Append found value of fpath field to the output list of paths belong to the host
	func(value any) error {
		path, ok := value.(string)
		// Check for invalid data
		if !ok {
			return fmt.Errorf("(MongoCli:LoadHostPaths:appender) type of %q field is %T, want - string",
				dbms.FieldFPath, value)
		}

		// Check path using match function
		if match(path) {
			// Append to the output list
			hostPaths = append(hostPaths, path)
		}

		// OK
		return nil
	})

	// Check for errors of loading fpath field
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:LoadHostPaths) cannot load host paths: %w", err)
	}

	// OK
	return hostPaths, nil
}

// loadFieldByPref append to caller output using value of the field from objects matched by filter
func (mc *Client) loadFieldByFilter(field string, filter *Filter, appendFunc func(any) error) error {
	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoObjsColl)

	// Make fields list
	fields := bson.D{{MongoFieldID, 1}}
	// Check for value of the requested field is not equal MongoFieldID,
	if field != MongoFieldID {
		// Append field to requested set
		fields = append(fields, bson.E{field, 1})
	}

	// Send request
	cursor, err := coll.Find(mc.Ctx, filter.Expr(), options.Find().
		// Include only the identifier and the required field
		SetProjection(fields))
	if err != nil {
		// Unexpected error
		return fmt.Errorf("(MongoCli:loadFieldByFilter) cannot load object field %q from %s.%s for host %q: %w",
			field, coll.Database().Name(), coll.Name(), mc.Cfg.CliHost, err)
	}
	defer func() {
		if err := cursor.Close(mc.Ctx); err != nil {
			log.E("(MongoCli:loadFieldByFilter) cannot close cursor: %v", err)
		}
	}()

	// Keep current termLong value to have ability to compare during long-term operations
	initTermLong := mc.TermLongVal

	// Make a list of results
	for cursor.Next(mc.Ctx) {
		// If value of the termLong was updated - need to terminate long-term operation
		if mc.TermLongVal != initTermLong {
			return fmt.Errorf("(MongoCli:loadFieldByFilter) terminated")
		}

		// Item to get ID and requested field from the query result
		var item map[string]string
		// Try to decode next cursor value to the map
		if err := cursor.Decode(&item); err != nil {
			return fmt.Errorf("(MongoCli:loadFieldByFilter) cannot decode cursor item: %w", err)
		}

		// Extract value
		value, ok := item[field]
		if !ok {
			// Looks like incorrect data from DB - requested field was found, extract record identifier
			if id, ok := item[MongoFieldID]; !ok {
				// Totally incorrect data, even mandatory identifier does not exist
				log.E("(MongoCli:loadFieldByFilter) incorrect data item loaded from DB - no %q and %q fields found: %#v",
					field, MongoFieldID, item)
			} else {
				// Print error about object without requested field
				log.E("(MongoCli:loadFieldByFilter) item with id %q does not contain %q field", id, field)
			}
			// Go to the next item
			continue
		}

		// Append requested field
		if err := appendFunc(value); err != nil {
			log.E("(MongoCli:loadFieldByFilter) cannot append data from item %#v - %v", item, err)
		}
	}

	// OK
	return nil
}
