package mongo

import (
	"fmt"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
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

func (mc *MongoClient) Commit() (int64, int64, error) {
	// Reset state on return
	defer func() {
		// Reset counters
		mc.updated = 0
		mc.deleted = 0
		// Reset lists of queued data
		mc.toDelete = nil
	}()

	// Check for keys to delete
	if nDel := len(mc.toDelete); nDel != 0 {
		log.D("(MongoCli:Commit) Need to delete %d keys", nDel)

		if mc.ReadOnly {
			panic("(MongoCli:Commit) ReadOnly is not supported")	// TODO
		} else {
			// TODO
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
func (mc *MongoClient) DeleteObj(fso *types.FSObject) error {
	return fmt.Errorf("DeleteObj - Not implemented")	// TODO
}
