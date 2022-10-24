package mongo

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/dbi/common"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoIDField	=	"_" + dbms.FieldID
	ObjsCollection	=	"objs"
)

//
// Agent client interface
//

func (mc *MongoClient) UpdateObj(fso *types.FSObject) error {
	// Push object to update queue

	if mc.ReadOnly {
		log.W("(MongoCli:UpdateObj) R/O mode IS SET, Insert/Update of %q collection" +
				" will NOT be performed => %s\n", ObjsCollection, mc.CliHost + ":" + fso.FPath)
		// Increase the update counter and return no errors
		mc.updated++
		// OK
		return nil
	}
	log.D("(MongoCli:UpdateObj) Insert/Update of collection %q => %s\n", ObjsCollection, mc.CliHost + ":" + fso.FPath)

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(ObjsCollection)

	// Update/Insert object
	id := common.MakeID(mc.CliHost, fso)
	doc := bson.D{{`$set`, bson.D{
		{MongoIDField,			id},
		{dbms.FieldHost,		mc.CliHost},
		{dbms.FieldName,		fso.Name},
		{dbms.FieldFPath,		fso.FPath},
		{dbms.FieldRPath,		fso.RPath},
		{dbms.FieldType,		fso.Type},
		{dbms.FieldSize,		fso.Size},
		{dbms.FieldMTime,		fso.MTime},
		{dbms.FieldChecksum,	fso.Checksum},
	}}}

	res, err := coll.UpdateOne(mc.Ctx,
		bson.D{{MongoIDField, id}},			// Update exactly this ID
		doc,
		options.Update().SetUpsert(true),	// do insert if no object with this ID was found
	)
	if err != nil {
		return fmt.Errorf("(MongoCli:UpdateObj) updateOne (id: %s) on %s.%s failed: %w",
				id, coll.Database().Name(), coll.Name(), err)
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

func (mc *MongoClient) DeleteObj(fso *types.FSObject) error {
	mongoID := common.MakeID(mc.CliHost, fso)

	if mc.ReadOnly {
		log.W("(MongoCli:DeleteObj) R/O mode IS SET, will not be performed: Delete => %s:%s (%s)\n",
			mc.CliHost, fso.FPath, mongoID)
	} else {
		log.D("(MongoCli:DeleteObj) Delete (pending) => %s:%s (%s)\n",
			mc.CliHost, fso.FPath, mongoID)
	}

	// XXX Append key to delete regardless of R/O mode because it will be skipped in the Commit() operation
	mc.toDelete = append(mc.toDelete, mongoID)

	// OK
	return nil
}

func (mc *MongoClient) Commit() (int64, int64, error) {
	// Reset state on return
	defer func() {
		// Reset counters
		mc.updated = 0
		mc.deleted = 0
		// Reset lists of queued data
		mc.toDelete = nil
	}()

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(ObjsCollection)

	// Create filter by identifiers
	filter := bson.D{{
		MongoIDField, bson.D{{ `$in`, mc.toDelete }},
	}}

	// Check for keys to delete
	if nDel := len(mc.toDelete); nDel != 0 {
		log.D("(MongoCli:Commit) Need to delete %d keys", nDel)

		if mc.ReadOnly {
			// Only load identifiers for all object that queued to deletion
			nd := []string{}	// will not be deleted
			wd := []string{}	// would be deleted

			if cursor, err := coll.Find(mc.Ctx, filter, options.Find().SetReturnKey(true)); err != nil {
				// Unexpected error
				log.E("(MongoCli:Commit) Find (used instead of Delete on R/O mode) on %s.%s for identifiers %v failed: %v",
					coll.Database().Name(), coll.Name(), mc.toDelete, err)

				// All identifers will NOT be deleted
				nd = append(nd, mc.toDelete...)
			} else {
				// Make a list of results
				var results []bson.M
				if err = cursor.All(mc.Ctx, &results); err != nil {
					// Unexpected error
					log.E("(MongoCli:Commit) Find.Cursor (used instead of Delete on R/O mode) on %s.%s for identifiers %v failed: %v",
						coll.Database().Name(), coll.Name(), mc.toDelete, err)

					// All identifers will NOT be deleted
					nd = append(nd, mc.toDelete...)
				}

				// Make a set from keys that should be deleted
				dset := tools.NewStrSet(mc.toDelete...)

				// Check all item from results
				for _, item := range results {
					// Extract ID field
					id, ok := item[MongoIDField].(string)
					if !ok {
						log.E("(MongoCli:Commit) Cannot convert object identifier to string, skip: %#v", item)
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
				mc.deleted = int64(len(wd))

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
			}
		} else {
			// Perform deletion
			res, err := coll.DeleteMany(mc.Ctx, filter)
			if err != nil {
				deleted := int64(0)
				if res != nil {
					deleted = res.DeletedCount
				}
				return 0, deleted, fmt.Errorf("(MongoCli:Commit) Delete from %s.%s failed: %w",
					coll.Database().Name(), coll.Name(), err)
			}

			mc.deleted = res.DeletedCount
		}

		log.D("(MongoCli:Commit) Done deletion operation")
	}

	// XXX Use intermediate variables to avoid resetting return values by deferred function
	ru, rd := mc.updated, mc.deleted

	return ru, rd, nil
}

func (mc *MongoClient) LoadHostPaths(match dbms.MatchStrFunc) ([]string, error) {
	return nil, fmt.Errorf("LoadHostPaths - Not implemented")	// TODO
}
