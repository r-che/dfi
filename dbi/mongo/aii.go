package mongo

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/common/tools"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/bson/primitive"
//	"go.mongodb.org/mongo-driver/mongo"
)

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

	fmt.Println(idkm)

	// 2. Select modification operator

	switch op {
	case dbms.Update:
		return mc.updateAII(args, idkm, add)
	case dbms.Delete:
		return mc.deleteAII(args, idkm)
	default:
		panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}

	// Unreachable
	return -1, -1, nil
}

func (mc *MongoClient) updateAII(args *dbms.AIIArgs, idkm types.IdKeyMap, add bool) (int64, int64, error) {
	ttu := int64(0)	// Total tags updated
	tdu := int64(0)	// Total descriptions updated
	var err error

	// List of fields that need to be loaded from DB
	rqFields := make([]string, 0, 2)

	// Update tags if exist
	if len(args.Tags) != 0 && add {
		// Need to load existing tags, add tags field
		rqFields = append(rqFields, dbms.AIIFieldTags)
	}

	// Update description if exist
	if args.Descr != "" && add {
		// Need to load existing description
		rqFields = append(rqFields, dbms.AIIFieldDescr)
	}

	var qr dbms.QueryResults
	// If some fields were requested - need to load existing AII about objects identified by ids
	if len(rqFields) != 0 {
		// Load existing objects
		if qr, err = mc.runSearch(MongoAIIColl, &dbms.QueryArgs{}, makeFilterIDs(idkm.Keys()), rqFields); err != nil {
			return 0, 0, fmt.Errorf("(MongoCli:updateAII) cannot load fields required for update: %w", err)
		}
	}

	// Process all found results
	// NOTE! qr is not empty only if:
	// - add is true
	// - objects specified by ids have any data for target field (description or tags are set)

	// Resulted map with tags and descriptions
	setAII := map[string]*dbms.AIIArgs{}

	for _, aii := range qr {
		itemData := dbms.AIIArgs{}

		//
		// Check for tags field exists
		//
		if tagsData, ok := aii[dbms.AIIFieldTags]; ok {
			// Check for correct type of tags
			tagsArr, ok := tagsData.(primitive.A)
			if !ok {
				log.E("MongoCli:updateAII) AII item contains invalid field %q - type of field is %T," +
					" want primitive.A (array), item: %#v", dbms.AIIFieldTags, tagsData, aii)
				// Skip this item
				goto nextAII
			}

			// Create set of tags from specified tags
			tags := tools.NewStrSet(args.Tags...)
			// Check and convert each tag from loaded tags data, then add to resulting set
			for _, tagVal := range tagsArr {
				tag, ok := tagVal.(string)
				if !ok {
					// Skip this item
					goto nextAII
				}

				// Add tag to the resulting set
				tags.Add(tag)
			}

			// Set tags to the item data
			itemData.Tags = tags.List()
		}

		//
		// Check for description field exists
		//
		if descrData, ok := aii[dbms.AIIFieldDescr]; ok {
			// Check for correct type of description
			descr, ok := descrData.(string)
			if !ok {
				log.E("MongoCli:updateAII) AII item contains invalid field %q - type of field is %T," +
					" want string, item: %#v", dbms.AIIFieldDescr, descrData, aii)
				// Skip this item
				goto nextAII
			}

			// Append new description value to the existing
			itemData.Descr = descr + tools.Tern(args.NoNL, `; `, "\n") + args.Descr
		}

		// Store item data
		setAII[aii[dbms.FieldID].(string)] = &itemData

		// Point to jump if something wrong with result
		nextAII:
	}

	// Check setAII for each identifier to find identifiers that were not handled in the preivous loop
	for _, id := range idkm.Keys() {
		// Get item
		if item := setAII[id]; item != nil {
			if len(args.Tags) != 0 && len(item.Tags) == 0 {
				// Set tags field
				item.Tags = args.Tags
			}

			if args.Descr != "" && item.Descr == "" {
				item.Descr = args.Descr
			}
		} else {
			// Need to set new values for this item
			setAII[id] = &dbms.AIIArgs{
				Tags:	args.Tags,
				Descr: args.Descr,
			}
		}
	}

	return -1, -1, fmt.Errorf("updateAII not implemented")	// TODO
	// OK
	return ttu, tdu, nil
}

func (mc *MongoClient) deleteAII(args *dbms.AIIArgs, idkm types.IdKeyMap) (int64, int64, error) {
	return -1, -1, fmt.Errorf("deleteAII not implemented")
}
