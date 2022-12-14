package mongo

import (
	"fmt"
	"strings"
	"regexp"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (mc *Client) GetAIIIds(withFields []string) ([]string, error) {
	// If no particular fields were requested
	if len(withFields) == 0 {
		// Use all user valuable fields
		withFields = dbms.UVAIIFields()
	}

	// Make fields filter
	fields := bson.D{}
	for _, field := range withFields {
		fields = append(fields, bson.E{
			field, bson.D{{`$exists`, true}},
		})
	}

	// Need to get AIIs which have any of the fields
	qr, err := mc.aggregateSearch(MongoAIIColl,
		// Join all fields by OR to match document that have at least one field from the fields set
		NewFilter().SetExpr(fields).JoinByOr(),
		 // Need to get only the identifier field
		[]string{dbms.FieldID})
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:GetAIIIds) cannot load identifiers of objects" +
			" that have filled AII fields %v: %w", withFields, err)
	}

	// Collect identifiers from the result
	ids := make([]string, 0, len(qr))
	for _, res := range qr {
		// Get object identifier, do not use safe type check because the value
		// of dbms.FieldID field already checked for string type in mc.aggregateSearch()
		ids = append(ids, res[dbms.FieldID].(string))
	}

	// OK
	return ids, nil
}

func (mc *Client) GetAIIs(ids, retFields []string) (dbms.QueryResultsAII, error) {
	// Load data for AII with requested identifiers
	qr, err := mc.aggregateSearch(MongoAIIColl, filterMakeIDs(ids), retFields)
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

		// Try create AII from requested fields from the result
		aii, err := fillAII(retFields, res)
		if err != nil {
			return result, fmt.Errorf("(MongoCli:GetAIIs) problem with object %v: %w", id, err)
		}

		result[id] = aii
	}

	return result, nil
}

func (mc *Client) ModifyAII(op dbms.DBOperator, args *dbms.AIIArgs, ids []string, add bool) (int64, int64, error) {
	// 1. Check for objects with identifiers ids really exist

	log.D("(MongoCli:ModifyAII) Check existing of objects to update AII ...")

	//
	// Check for all objects with specified identifiers exist
	//

	qr, err := mc.aggregateSearch(MongoObjsColl, filterMakeIDs(ids), []string{dbms.FieldID})
	if err != nil {
		return 0, 0, fmt.Errorf("(MongoCli:ModifyAII) %w", err)
	}

	// Check for all ids were found and create a map with correspondence between the object key and its ID
	sIds := tools.NewSet(ids...)
	idkm := make(types.IDKeyMap, len(ids))
	for objKey, r := range qr {
		id := r[dbms.FieldID].(string)

		// Remove ID from query result from the set of required identifiers
		sIds.Del(id)

		// Add correspondence between identifier and object key
		idkm[id] = objKey
	}

	if !sIds.Empty() {
		// Some identifiers were not found
		return 0, 0, fmt.Errorf("(MongoCli:ModifyAII) the following identifiers do not exist in DB: %s",
			strings.Join(sIds.Sorted(), " "))
	}

	log.D("(MongoCli:ModifyAII) OK - all required objects exist")

	// 2. Select modification operator

	//nolint:exhaustive	// panic uncovers any forgotten operators
	switch op {
	case dbms.Update:
		return mc.updateAII(args, idkm, add)
	case dbms.Delete:
		return mc.deleteAII(args, idkm)
	// No mo operators supported on AII
	default:
		panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}
}

func (mc *Client) updateAII(args *dbms.AIIArgs, idkm types.IDKeyMap, add bool) (int64, int64, error) {
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
		rqFields = make([]string, 0, 1)	// at least one field should be requested

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
	qr, err := mc.runSearch(MongoAIIColl, &dbms.QueryArgs{}, filterMakeIDs(idkm.Keys()), rqFields)
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

func (mc *Client) appendAII(args *dbms.AIIArgs, idkm types.IDKeyMap, qr dbms.QueryResults) (int64, int64, error) {
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
		tu, du, err := mergeAII(args, aii, &doc)
		if err != nil {
			log.E("(MongoCli:appendAII) %v", err)
			continue
		}

		// Check for no fields in the document
		if len(doc) == 0 {
			// No updates are required for this document
			continue
		}

		// Do update/insert
		res, err := coll.UpdateOne(mc.Ctx,
			bson.D{{MongoFieldID, id}},			// Update exactly this ID
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

func (mc *Client) setAII(args *dbms.AIIArgs, idkm types.IDKeyMap, qr dbms.QueryResults) (int64, int64, error) {
	// Need to set tags/descr field to all items

	var ttu, tdu int64	// total tags/total descriptions updated

	// Make a set with identifiers for which no records were found in the AII collection
	needInsert := tools.NewSet(idkm.Keys()...)	// put all identifiers scheduled to update
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
		ttu = int64(len(idkm) - needInsert.Len())
	}

	// Is description provided?
	if args.Descr != "" {
		// Append description
		fields = append(fields, bson.E{`$set`, bson.D{{dbms.AIIFieldDescr, args.Descr}}})
		// Update counter
		tdu = int64(len(idkm) - needInsert.Len())
	}

	// Check that NOT all AII have to be inserted - some need to be updated
	if needInsert.Len() != len(idkm) {
		if err := mc.setAIIsVals(idkm, fields); err != nil {
			return 0, 0, fmt.Errorf("(MongoCli:setAII) %w", err)
		}
	}

	log.D("(MongoCli:setAII) %d AII record(s) were successfully set", len(idkm) - needInsert.Len())

	// Check for nothing to insert
	if needInsert.Empty() {
		// OK, no insertions required
		return ttu, tdu, nil
	}

	//
	// Insert fields that currently do not exist in the AII collection
	//

	log.D("(MongoCli:setAII) Inserting new AII records...")

	// Update/Insert AII, do this one by one because UpdateMany() does not support SetUpsert(true) option
	for _, id := range needInsert.Sorted() {
		// Insert one document
		if err := mc.insertAII(id, idkm, fields); err != nil {
			return ttu, tdu, fmt.Errorf("(MongoCli:setAII) %w", err)
		}


		// Update counters
		ttu += tools.Tern(args.Tags != nil, int64(1), 0)
		tdu += tools.Tern(args.Descr != "", int64(1), 0)
	}

	log.D("(MongoCli:setAII) %d AII record(s) were successfully inserted", needInsert.Len())

	// OK
	return ttu, tdu, nil
}

func (mc *Client) setAIIsVals(idkm types.IDKeyMap, fields bson.D) error {
	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoAIIColl)

	// Ok, some objects already in the AII collection, update them
	res, err := coll.UpdateMany(
		mc.Ctx,
		filterMakeIDs(idkm.Keys()).Expr(),
		fields,
	)

	if err != nil {
		return fmt.Errorf("updateMany (ids: %s) on %s.%s failed: %w",
				idkm.Keys(), coll.Database().Name(), coll.Name(), err)
	}

	if res.MatchedCount == 0 && res.UpsertedCount == 0 {
		return fmt.Errorf("updateMany (ids: %s) on %s.%s returned success," +
			" but no documents were changed", idkm.Keys(), coll.Database().Name(), coll.Name())
	}

	// OK
	return nil
}
func (mc *Client) insertAII(id string, idkm types.IDKeyMap, fields bson.D) error {
	// Create a new document
	doc := bson.D{{`$set`, bson.D{
		{MongoFieldID,			id},
		{dbms.AIIFieldHost,		idkm[id].Host},
		{dbms.AIIFieldFPath,	idkm[id].Path},
	}}}

	// Add fields that have to beset
	doc = append(doc, fields...)

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoAIIColl)

	// Do update/insert
	res, err := coll.UpdateOne(mc.Ctx,
		bson.D{{MongoFieldID, id}},			// Update exactly this ID
		doc,
		options.Update().SetUpsert(true),	// do insert if no object with this ID was found
	)

	if err != nil {
		return fmt.Errorf("(MongoCli:insertAII) updateOne (id: %s) on %s.%s failed: %w",
				id, coll.Database().Name(), coll.Name(), err)
	}

	if res.MatchedCount == 0 && res.UpsertedCount == 0 {
		return fmt.Errorf("(MongoCli:insertAII) updateOne (id: %s) on %s.%s returned success," +
			" but no documents were changed", id, coll.Database().Name(), coll.Name())
	}

	// OK
	return nil
}

func (mc *Client) deleteAII(args *dbms.AIIArgs, idkm types.IDKeyMap) (int64, int64, error) {
	var err error

	td := int64(0)	// Tags deleted
	dd := int64(0)	// Descriptions deleted

	// Delete tags if requested
	if args.Tags != nil {
		// Check for first tags for ALL value
		if args.Tags[0] == dbms.AIIAllTags {
			// Need to clear all tags
			td, err = mc.clearAIIField(dbms.AIIFieldTags, idkm.Keys())
		} else {
			// Need to remove the separate tags
			td, err = mc.delTags(args.Tags, idkm)
		}

		if err != nil {
			return td, dd, fmt.Errorf("(MongoCli:deleteAII) %w", err)
		}
	}

	// Delete description if requested
	if args.Descr == dbms.AIIDelDescr {
		// Clear description for selected identifiers
		dd, err = mc.clearAIIField(dbms.AIIFieldDescr, idkm.Keys())
		if err != nil {
			return td, dd, fmt.Errorf("(MongoCli:deleteAII) %w", err)
		}
	}

	// OK
	return td, dd, nil
}

func (mc *Client) delTags(tags []string, idkm types.IDKeyMap) (int64, error) {
	// Convert list of tags to set to check existing tags for need to be deleted
	toDelTags := tools.NewSet(tags...)

	tu := int64(0) // Total changed values of tags fields

	log.D("(MongoCli:delTags) Collecting AII existing tags")

	qr, err := mc.GetAIIs(idkm.Keys(), []string{dbms.AIIFieldTags})
	if err != nil {
		return 0, fmt.Errorf("(MongoCli:delTags) cannot load existing tags values: %w", err)
	}

	// List of ID for which tags field should be cleared
	var clearTags []string

	// Get collection handler
	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoAIIColl)

	// Do for each loaded result
	for id, aii := range qr {
		// Select tags to keep
		keepTags := toDelTags.Complement(aii.Tags...)

		// Check for length of existing tags is the same that keep
		if len(aii.Tags) == len(keepTags) {
			// No tags should be removed from this item, skip
			log.D("(MongoCli:delTags) No tags update required for %s", id)
			continue
		}

		// If empty list of keep tags - need to remove (unset) tags field
		if len(keepTags) == 0 {
			// Add identifier to queue
			clearTags = append(clearTags, id)
			// Skip this item
			continue
		}

		// Need to set new value of tags field value without removed tags
		res, err := coll.UpdateOne(mc.Ctx,
			bson.D{{MongoFieldID, id}},									// set filter
			bson.D{{`$set`, bson.D{{dbms.AIIFieldTags, keepTags}}}})	// set field value
		if err != nil {
				return tu, fmt.Errorf("(MongoCli:delTags) cannot remove tags %v from %q: %w", tags, id, err)
		}

		if res.MatchedCount == 0 && res.ModifiedCount == 0 {
			return tu, fmt.Errorf("(MongoCli:delTags) updateOne (id: %s) on %s.%s returned success," +
				" but no documents were changed", id, coll.Database().Name(), coll.Name())
		}

		// Success, increase updated tags counter
		tu++
	}

	if len(clearTags) == 0 {
		// All done, return
		return tu, nil
	}

	// Need to remove tags fields from documents enumerated in clearTags
	td, err := mc.clearAIIField(dbms.AIIFieldTags, clearTags)
	if err != nil {
		return tu, fmt.Errorf("(MongoCli:delTags) cannot delete tags %v from %v: %w", tags, clearTags, err)
	}

	// OK
	return tu + td, nil
}

func (mc *Client) clearAIIField(field string, ids []string) (int64, error) {
	// List of keys that can be safely deleted to clearing field
	toDelAII := make([]string, 0, len(ids))
	// List of keys on which only the field should be deleted
	toDelField := make([]string, 0, len(ids))

	// Total cleared
	tc := int64(0)

	//
	// Need to load field names for each id
	//

	log.D("(MongoCli:clearAIIField) Collecting AII info to clearing field %q on %v...", field, ids)

	qr, err := mc.aggregateSearch(MongoAIIColl, filterMakeIDs(ids), nil)
	if err != nil {
		return 0, fmt.Errorf("(MongoCli:clearAIIField) cannot load AII objects fields: %w", err)
	}

	// Enumerate all resuts to check that all fields except identifier + required fields need to be deleted
	for _, aii := range qr {
		// Get all fields from AII
		fields := tools.NewSet[string]()
		for field := range aii {
			fields.Add(field)
		}

		// Clean fields
		fields.Del(field).				// delete field that should be cleared
			Del(dbms.FieldID).			// delete field with identifier
			Del(objMandatoryFields...)	// delete all non-AII mandatory field

		// Check for fields is empty
		if fields.Empty() {
			// Then AII with this ID can be removed completely
			toDelAII = append(toDelAII, aii[dbms.FieldID].(string))
		} else {
			// Only field has to be removed, because some other valuable field present in AII
			toDelField = append(toDelField, aii[dbms.FieldID].(string))
		}
	}

	// Is fields should be removed from documents?
	if len(toDelField) != 0 {
		// Delete them
		log.D("(MongoCli:clearAIIField) Clearing field %q in: %v", field, toDelField)
		td, err := mc.delFieldByID(MongoAIIColl, field, toDelField)
		if err != nil {
			return tc, fmt.Errorf("(MongoCli:clearAIIField) cannot clear: %w", err)
		}

		// Increase cleared counter
		tc += td
	}

	// Is AII documents should be deleted?
	if len(toDelAII) != 0 {
		// Delete them
		dn, err := mc.deleteAIIs(toDelAII)
		if err != nil {
			return tc, fmt.Errorf("(MongoCli:clearAIIField) %w", err)
		}

		// Increase cleared counter
		tc += dn
	}

	if tc == 0 {
		log.D("(MongoCli:clearAIIField) Nothing to clear")
	}

	// OK
	return tc, nil
}

func (mc *Client) deleteAIIs(ids []string) (int64, error) {
	// Delete them
	log.D("(MongoCli:deleteAIIs) Removing AIIs: %v", ids)

	coll := mc.c.Database(mc.Cfg.ID).Collection(MongoAIIColl)

	res, err := coll.DeleteMany(mc.Ctx, filterMakeIDs(ids).Expr())
	if err != nil {
		return 0, fmt.Errorf("(MongoCli:deleteAIIs) cannot remove AIIs with ids %v: %w", ids, err)
	}

	if res.DeletedCount == 0 {
		return 0, fmt.Errorf("(MongoCli:deleteAIIs) deleteMany (ids: %v) on %s.%s returned success," +
			" but no documents were changed", ids, coll.Database().Name(), coll.Name())
	}

	// OK
	return res.DeletedCount, nil
}

func (mc *Client) QueryAIIIds(qa *dbms.QueryArgs) ([]string, error) {
	// AII fields filter
	aiiFilter := NewFilter()

	// Check for need to use tags
	if qa.UseTags {
		// Append filters by tags
		for _, phrase:= range qa.SP {
			aiiFilter.Append(bson.E{dbms.AIIFieldTags,
				primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}})
		}
	}

	// Check for need to use description
	if qa.UseDescr {
		// Append filters by description
		for _, phrase:= range qa.SP {
			aiiFilter.Append(bson.E{dbms.AIIFieldDescr,
					primitive.Regex{Pattern: regexp.QuoteMeta(phrase), Options: "i"}})
		}
	}

	// Join created filter
	aiiFilter = aiiFilter.JoinByOr()

	// Run search to select identifiers of objects with matched fields
	log.D("(MongoCli:QueryAIIIds) Run regexp-based search by AII collection using filter %v", aiiFilter)

	qr, err := mc.aggregateSearch(MongoAIIColl,
		aiiFilter,				// filter by fields
		[]string{dbms.FieldID}) // get only the identifier field
	if err != nil {
		return nil, fmt.Errorf("(MongoCli:QueryAIIIds) cannot load identifiers of objects" +
			" matched by AII fields: %w", err)
	}

	// Collect identifiers from the result
	ids := make([]string, 0, len(qr))
	for _, res := range qr {
		// Get object identifier, do not use safe type check because the value
		// of dbms.FieldID field already checked for string type in mc.aggregateSearch()
		ids = append(ids, res[dbms.FieldID].(string))
	}

	// OK
	return ids, nil
}
