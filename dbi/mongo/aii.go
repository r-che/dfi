package mongo

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (mc *MongoClient) GetAIIs(ids, retFields []string) (dbms.QueryResultsAII, error) {
	// Load data for AII with requested identifiers
	qr, err := mc.aggregateSearch(MongoAIIColl, makeFilterIDs(ids), retFields)
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:GetAIIs) %w", err)
	}

	// Prepare output value
	result := make(dbms.QueryResultsAII, len(ids))

	// Convert all QRItem to AIIArgs values
	for _, res := range qr {
		// Get object identifier, do not use safe type check because the value
		// of dbms.FieldID field already checked for string type in mc.aggregateSearch()
		id := res[dbms.FieldID].(string)

		// Allocate new AII structure
		aii := &dbms.AIIArgs{}
		// Try to get all requested fields from the result
		for _, field := range retFields {
			// Get field value
			v, ok := res[field]
			if !ok {
				// Field was not found
				continue
			}

			// Choose correct type
			switch v.(type) {
			// Simple string value
			case string:
				if err := aii.SetFieldStr(field, v.(string)); err != nil {
					return result, fmt.Errorf("(MongoCli:GetAIIs) cannot set field from result: %w", err)
				}
			// List (slice) of string values
			case primitive.A:
				// Convert to string list
				strList, err := primArrToStrList(v)
				if err != nil {
					return result, fmt.Errorf("(MongoCli:GetAIIs) problem to handle field %q of object %s: %w",
						field, id, err)
				}
				if err := aii.SetFieldList(field, strList); err != nil {
					return result, fmt.Errorf("(MongoCli:GetAIIs) cannot set field from result: %w", err)
				}
			// Unsupported type
			default:
				return result, fmt.Errorf("(MongoCli:GetAIIs) the field %q of object %s contains" +
					" unsupported type %T of value %#v", field, id, v, v)
			}
		}

		result[id] = aii
	}

	return result, nil
}

func (mc *MongoClient) ModifyAII(op dbms.DBOperator, args *dbms.AIIArgs, ids []string, add bool) (int64, int64, error) {
	// 1. Check for objects with identifiers ids really exist

	log.D("(MongoCli:ModifyAII) Check existing of objects to update AII ...")

	//
	// Check for all objects with specified identifiers exist
	//

	qr, err := mc.aggregateSearch(MongoObjsColl, makeFilterIDs(ids), []string{dbms.FieldID})
	if err != nil {
		return 0, 0, fmt.Errorf("(MongoCli:ModifyAII) %w", err)
	}

	// Check for all ids were found and create a map with correspondence between the object key and its ID
	sIds := tools.NewStrSet(ids...)
	idkm := make(types.IdKeyMap, len(ids))
	for objKey, r := range qr {
		id := r[dbms.FieldID].(string)

		// Remove ID from query result from the set of required identifiers
		sIds.Del(id)

		// Add correspondence between identifier and object key
		idkm[id] = objKey
	}

	if len(*sIds) != 0 {
		// Some identifiers were not found
		return 0, 0, fmt.Errorf("(MongoCli:ModifyAII) the following identifiers do not exist in DB: %s",
			strings.Join(sIds.List(), " "))
	}

	log.D("(MongoCli:ModifyAII) OK - all required objects exist")

	// 2. Select modification operator

	switch op {
	case dbms.Update:
		return mc.updateAII(args, idkm, add)
	case dbms.Delete:
		return mc.deleteAII(args, idkm)
	default:
		panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}
}

func (mc *MongoClient) updateAII(args *dbms.AIIArgs, idkm types.IdKeyMap, add bool) (int64, int64, error) {
	// Check for no fields required to set
	if args.Tags == nil && args.Descr == "" {
		return 0, 0, fmt.Errorf("(MongoCli:updateAII) no fields required to set")
	}

	//
	// Get information about existing AII
	//

	// List of fields that need to be loaded from DB
	var rqFields []string

	// If need to add information to existing items
	if add {
		// Need to load existing values of the fields
		rqFields := make([]string, 0, 2)

		// Check for tags field should be updated by adding new tags set to the existing set
		if args.Tags != nil && add {
			// Add the tags field name to the requested fields set to get existing tags
			rqFields = append(rqFields, dbms.AIIFieldTags)
		}

		// Check for description field should be updated by adding new value to existing
		if args.Descr != "" && add {
			// Add the description field name to the requested fields set to get existing description
			rqFields = append(rqFields, dbms.AIIFieldDescr)
		}
	} else {
		// Need to set (overwrite if items already exist) information in AII,
		// no need to load existing information, only object's identifiers are required
		rqFields = []string{dbms.FieldID}
	}

	// Load existing objects
	var qr dbms.QueryResults
	qr, err := mc.runSearch(MongoAIIColl, &dbms.QueryArgs{}, makeFilterIDs(idkm.Keys()), rqFields)
	if err != nil {
		return 0, 0, fmt.Errorf("(MongoCli:updateAII) cannot load fields required for update: %w", err)
	}

	//
	// Perform update/insert operations
	//

	// Check for new values should be appended to existing
	if add {
		return mc.appendAII(args, idkm, qr)
	}

	// Just update them otherwise
	return mc.setAII(args, idkm, qr)
}

func (mc *MongoClient) appendAII(args *dbms.AIIArgs, idkm types.IdKeyMap, qr dbms.QueryResults) (int64, int64, error) {
	var ttu, tdu int64 // total tags/decription updates counters

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoAIIColl)

	for id, key := range idkm {
		// Make set of fields that need to be set
		doc := bson.D{}

		// Get AII for the object with key from query results
		aii := qr[key]
		// Check for nothing was loaded from DB for such object
		if aii == nil {
			// Currently, no AII exists in DB for this object, use empty map
			aii = dbms.QRItem{}

			// Add required fields to new AII document
			doc = append(doc, bson.E{`$set`, bson.D{
				{dbms.AIIFieldHost, key.Host},
				{dbms.AIIFieldFPath, key.Path},
			}})
		}

		// Tags/description updated counters for the current item
		var tu, du int64

		// Get list of tags which have to be set
		if tags, err := addTags(args, aii); err != nil {
			log.E("(MongoCli:appendAII) cannot add/set tags: %v", err)
			continue
		} else if tags != nil {
			// Append tags field
			doc = append(doc, bson.E{`$set`, bson.D{{dbms.AIIFieldTags, tags}}})
			// Increase counter of tags updates
			tu++
		}

		// Add/set description
		if descr, err := addDescr(args, aii); err != nil {
			log.E("(MongoCli:appendAII) cannot add/set description: %v", err)
			continue
		} else if descr != "" {
			// Append description
			doc = append(doc, bson.E{`$set`, bson.D{{dbms.AIIFieldDescr, descr}}})
			// Increase counter of description updates
			du++
		}

		// Check for no fields in the document
		if len(doc) == 0 {
			// No updates are required for this document
			continue
		}

		// Do update/insert
		res, err := coll.UpdateOne(mc.Ctx,
			bson.D{{MongoIDField, id}},			// Update exactly this ID
			doc,
			options.Update().SetUpsert(true),
		)

		if err != nil {
			return ttu, tdu, fmt.Errorf("(MongoCli:appendAII) updateOne (id: %s) on %s.%s failed: %w",
					id, coll.Database().Name(), coll.Name(), err)
		}

		if res.MatchedCount == 0 && res.UpsertedCount == 0 {
			return ttu, tdu, fmt.Errorf("(MongoCli:appendAII) updateOne (id: %s) on %s.%s returned success," +
				" but no documents were changed", id, coll.Database().Name(), coll.Name())
		}

		// Update counters
		ttu += tu
		tdu += du
	}

	log.D("(MongoCli:appendAII) Data update (append) completed for %s, updated - %d tags, %d descriptions",
		idkm.Keys(), ttu, tdu)

	// OK
	return ttu, tdu, nil
}

func (mc *MongoClient) setAII(args *dbms.AIIArgs, idkm types.IdKeyMap, qr dbms.QueryResults) (int64, int64, error) {
	// Need to set tags/descr field to all items

	var ttu, tdu int64	// total tags/total descriptions updated

	// Make a set with identifiers for which no records were found in the AII collection
	needInsert := tools.NewStrSet(idkm.Keys()...)	// put all identifiers scheduled to update
	// Then remove the existing ones in the AII collection
	for _, v := range qr {
		needInsert.Del(v[dbms.FieldID].(string))
	}

	//
	// Make a set of fields with values that need to be updated
	//
	fields := bson.D{}

	// Is tags provided?
	if args.Tags != nil {
		// Append tags
		fields = append(fields, bson.E{`$set`, bson.D{{dbms.AIIFieldTags, args.Tags}}})
		// Update counter
		ttu = int64(len(idkm) - len(*needInsert))
	}

	// Is description provided?
	if args.Descr != "" {
		// Append description
		fields = append(fields, bson.E{`$set`, bson.D{{dbms.AIIFieldDescr, args.Descr}}})
		// Update counter
		tdu = int64(len(idkm) - len(*needInsert))
	}

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoAIIColl)

	// Check that NOT all AII have to be inserted - some need to be updated
	if len(*needInsert) != len(idkm) {
		// Ok, we some objects in AII collection, update them
		res, err := coll.UpdateMany(
			mc.Ctx,
			makeFilterIDs(idkm.Keys()),
			fields,
		)

		if err != nil {
			return 0, 0, fmt.Errorf("(MongoCli:setAII) updateMany (ids: %s) on %s.%s failed: %w",
					idkm.Keys(), coll.Database().Name(), coll.Name(), err)
		}

		if res.MatchedCount == 0 && res.UpsertedCount == 0 {
			return 0, 0, fmt.Errorf("(MongoCli:UpdateObj) updateMany (ids: %s) on %s.%s returned success," +
				" but no documents were changed", idkm.Keys(), coll.Database().Name(), coll.Name())
		}
	}

	log.D("(MongoCli:setAII) %d AII record(s) were successfuly set", len(idkm) - len(*needInsert))

	// Check for nothing to insert
	if len(*needInsert) == 0 {
		// OK, no insertions required
		return ttu, tdu, nil
	}

	//
	// Insert fields that currently do not exist in the AII collection
	//

	log.D("(MongoCli:setAII) Inserting new AII records...")

	// Update/Insert AII, do this one by one because UpdateMany() does not support SetUpsert(true) option
	for _, id := range needInsert.List() {
		// Create a new document
		doc := bson.D{{`$set`, bson.D{
			{MongoIDField,			id},
			{dbms.AIIFieldHost,		idkm[id].Host},
			{dbms.AIIFieldFPath,	idkm[id].Path},
		}}}

		// Add fields that have to beset
		doc = append(doc, fields...)

		// Do update/insert
		res, err := coll.UpdateOne(mc.Ctx,
			bson.D{{MongoIDField, id}},			// Update exactly this ID
			doc,
			options.Update().SetUpsert(true),	// do insert if no object with this ID was found
		)

		if err != nil {
			return ttu, tdu, fmt.Errorf("(MongoCli:setAII) updateOne (id: %s) on %s.%s failed: %w",
					id, coll.Database().Name(), coll.Name(), err)
		}

		if res.MatchedCount == 0 && res.UpsertedCount == 0 {
			return ttu, tdu, fmt.Errorf("(MongoCli:setAII) updateOne (id: %s) on %s.%s returned success," +
				" but no documents were changed", id, coll.Database().Name(), coll.Name())
		}

		// Update counters
		ttu += tools.Tern(args.Tags != nil, int64(1), 0)
		tdu += tools.Tern(args.Descr != "", int64(1), 0)
	}

	log.D("(MongoCli:setAII) %d AII record(s) were successfuly inserted", len(*needInsert))

	// OK
	return ttu, tdu, nil
}

func (mc *MongoClient) deleteAII(args *dbms.AIIArgs, idkm types.IdKeyMap) (int64, int64, error) {
	// TODO
	return -1, -1, fmt.Errorf("deleteAII not implemented")
}
