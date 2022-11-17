//go:build dbi_redis
package redis

import (
	"fmt"
	"strings"
	"errors"

	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/log"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"

    rsh "github.com/RediSearch/redisearch-go/redisearch"
)

func (rc *RedisClient) ModifyAII(op dbms.DBOperator, args *dbms.AIIArgs, ids []string, add bool) (int64, int64, error) {
	// 0. Get RediSearch client
	rsc, err := rc.rschInit(metaRschIdx)
	if err != nil {
		return 0, 0, fmt.Errorf("(RedisCli:ModifyAII) cannot initialize RediSearch client: %w", err)
	}

	// 1. Check for objects with identifiers ids really exist

	log.D("(RedisCli:ModifyAII) Loading object keys for provided identifiers...")

	// Empty query arguments - no special search parameters are required
	qa := &dbms.QueryArgs{}
	// Create RediSearch query to get identifiers
	q := rsh.NewQuery(rshQueryByIds(ids, qa))
	// Run search to get results by IDs
	qr, err := rshSearch(rsc, q, []string{dbms.FieldID})
	if err != nil {
		return 0, 0, fmt.Errorf("(RedisCli:ModifyAII) search failed: %w", err)
	}

	// Create IDs set, which should keep the not found identifiers
	nf := tools.NewStrSet(ids...)

	// Create map indentifiers found in DB
	fids := make(types.IdKeyMap, len(ids))
	for k, v := range qr {
		id, ok := v[dbms.FieldID]
		if !ok {
			log.E("(RedisCli:ModifyAII) Loaded invalid object from DB - no ID field (%q) was found: %s:%s", dbms.FieldID, k.Host, k.Path)
			continue
		}

		// Convert ID to string representation
		sid, ok := id.(string)
		if !ok {
			log.E("(RedisCli:ModifyAII) Loaded invalid object from DB - " +
				"ID field (%q) cannot be converted to string: %s:%s => %v", dbms.FieldID, k.Host, k.Path, id)
		}

		// Save key associated with the ID
		fids[sid] = k
		// Remove this ID from the set of not-found identifiers
		nf.Del(sid)
	}

	if len(*nf) != 0 {
		return 0, 0, fmt.Errorf("(RedisCli:ModifyAII) the following identifiers do not exist in DB: %s", strings.Join(nf.List(), " "))
	}

	// 2. Run modification operator

	switch op {
	case dbms.Update:
		return rc.updateAII(args, fids, add)
	case dbms.Delete:
		return rc.deleteAII(args, fids)
	default:
		panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}
}

func (rc *RedisClient) GetAIIIds(withFields []string) ([]string, error) {
	// Set of unique identifiers
	ids := tools.NewStrSet()

	// If no particular fields were requested
	if len(withFields) == 0 {
		// Use all user valuable fields
		withFields = dbms.UVAIIFields()
	}

	// Load AII identifiers that have fields from withFields set
	for _, field := range withFields {
		setKey := RedisAIIDSetPrefix + field

		set, err := rc.c.SMembers(rc.Ctx, setKey).Result()
		if err != nil {
			return nil, fmt.Errorf("(RedisCli:GetAIIIds) cannot load identifiers of objects with filled %q field: %w", field, err)
		}

		ids.Add(set...)
	}

	return ids.List(), nil
}

func (rc *RedisClient) GetAIIs(ids, retFields []string) (dbms.QueryResultsAII, error) {
	result := make(dbms.QueryResultsAII, len(ids))

	// Get requested fields for each ID
	for _, id := range ids {
		key := RedisAIIPrefix + id

		res, err := rc.c.HGetAll(rc.Ctx, key).Result()
		if err != nil {
			return result, fmt.Errorf("(RedisCli:GetAIIs) cannot get AII for %q: %w", key, err)
		}

		// Check for nothing was found
		if len(res) == 0 {
			// No AII data for this id
			continue
		}

		// Allocate new AII structure
		aii := &dbms.AIIArgs{}
		// Try to get all requested fields from the result
		for _, field := range retFields {
			v, ok := res[field]
			if !ok {
				// Field was not found
				continue
			}

			// Select field
			if err := aii.SetFieldStr(field, v); err != nil {
				return result, fmt.Errorf("(RedisCli:GetAIIs) cannot set field from result: %w", err)
			}
		}

		result[id] = aii
	}

	// OK
	return result, nil
}

func (rc *RedisClient) updateAII(args *dbms.AIIArgs, ids types.IdKeyMap, add bool) (int64, int64, error) {
	var err error

	ttu := int64(0)	// Total tags updated
	tdu := int64(0)	// Total descriptions updated

	// Update tags if exist
	if args.Tags != nil {
		if add {
			ttu, err = rc.addTags(args.Tags, ids)
		} else {
			ttu, err = rc.setTags(args.Tags, ids)
		}

		if err != nil {
			return ttu, tdu, fmt.Errorf("(RedisCli:updateAII) %w", err)
		}
	}

	// Update description if exist
	if args.Descr != "" {
		if add {
			tdu, err = rc.addDescr(args.Descr, ids, args.NoNL)
		} else {
			tdu, err = rc.setDescr(args.Descr, ids)
		}

		if err != nil {
			return ttu, tdu, fmt.Errorf("(RedisCli:updateAII) %w", err)
		}
	}

	// OK
	return ttu, tdu, nil
}

func (rc *RedisClient) addTags(tags []string, ids types.IdKeyMap) (int64, error) {
	tu := int64(0)	// Total updated tags fields

	// Do for each identifier
	for id, objKey := range ids {
		// AII key
		key := RedisAIIPrefix + id

		// Make a set of tags that should be set to the id
		allTags := tools.NewStrSet(tags...)

		// Load existing values of tags field
		tagsStr, err := rc.c.HGet(rc.Ctx, key, dbms.AIIFieldTags).Result()
		if err == nil {
			// Tags field extracted, make union between extracted existing tags and new tags
			allTags.Add(strings.Split(tagsStr, ",")...)
		} else if errors.Is(err, RedisNotFound) {
			// Ok, currently no tags for this object, nothing to do
		} else {
			// Something went wrong
			return tu, fmt.Errorf("(RedisCli:addTags) cannot get tags field %q for key %q: %w", dbms.AIIFieldTags, key, err)
		}

		// Compare existing tags and new set
		if tagsStr == strings.Join(allTags.List(), ",") {
			// Skip update
			log.D("(RedisCli:addTags) No tags update required for %s", id)
			continue
		}

		// Set tags for the current identifier
		if _, err := rc.setTags(allTags.List(), types.IdKeyMap{id: objKey}); err != nil {
			return tu, err
		}

		tu++
	}

	// OK
	return tu, nil
}

func (rc *RedisClient) setTags(tags []string, ids types.IdKeyMap) (int64, error) {
	// Make tags field value
	tagsVal := strings.Join(tags, ",")

	log.D("(RedisCli:setTags) %q => %s", tagsVal, ids)

	ts := int64(0)	// Total tags set

	// Do for each identifier
	for id, objKey := range ids {
		if err := rc.setAIIField(id, dbms.AIIFieldTags, tagsVal, objKey); err != nil {
			return ts, err
		}
		ts++
	}

	// Update index of objects that use tags field
	idxSet := RedisAIIDSetPrefix + dbms.AIIFieldTags
	if n, err := rc.c.SAdd(rc.Ctx, idxSet, ids.KeysAny()...).Result(); err != nil {
		log.E("(RedisCli:setTags) cannot add identifiers %s to set %q: %v - " +
				"the search result may be incomplete", ids, idxSet, err)
	} else {
		log.D("(RedisCli:setTags) Added %d members to set %q", n, idxSet)
	}

	// OK
	return ts, nil
}

func (rc *RedisClient) addDescr(descr string, ids types.IdKeyMap, noNL bool) (int64, error) {
	tu := int64(0)	// Total updated description fields

	// Do for each identifier
	for id, objKey := range ids {
		// AII key
		key := RedisAIIPrefix + id

		// Full description
		var fullDescr string

		// Load existing values of description field
		if oldDescr, err := rc.c.HGet(rc.Ctx, key, dbms.AIIFieldDescr).Result(); err == nil {
			// Append new description line to existing
			if noNL {
				fullDescr = oldDescr + "; " + descr
			} else {
				fullDescr = oldDescr + "\n" + descr
			}
		} else if errors.Is(err, RedisNotFound) {
			// Ok, currently no description for this object
			fullDescr = descr
		} else {
			// Something went wrong
			return tu, fmt.Errorf("(RedisCli:addDescr) cannot get description field %q for key %q: %w", dbms.AIIFieldDescr, key, err)
		}

		// Set description for the current identifier
		n, err := rc.setDescr(fullDescr, types.IdKeyMap{id: objKey})
		if err != nil {
			return tu, err
		}

		tu += n
	}

	// OK
	return tu, nil
}

func (rc *RedisClient) setDescr(descr string, ids types.IdKeyMap) (int64, error) {
	tu := int64(0)	// Total updated description fields

	// Do for each identifier
	for id, objKey := range ids {
		if err := rc.setAIIField(id, dbms.AIIFieldDescr, descr, objKey); err != nil {
			return tu, err
		}

		tu++
	}

	// Update index of objects that use description field
	idxSet := RedisAIIDSetPrefix + dbms.AIIFieldDescr
	if n, err := rc.c.SAdd(rc.Ctx, idxSet, ids.KeysAny()...).Result(); err != nil {
		log.E("(RedisCli:setDescr) cannot add identifiers %s to set %q: %v - " +
				"the search result may be incomplete", ids, idxSet, err)
	} else {
		log.D("(RedisCli:setDescr) Added %d members to set %q", n, idxSet)
	}

	// OK
	return tu, nil
}

func (rc *RedisClient) setAIIField(id, field, value string, objKey types.ObjKey) error {
	// Set tags
	res := rc.c.HSet(rc.Ctx,
		// Make AII key in 'aii:OBJECT_ID' format
		RedisAIIPrefix + id,
		// Set field
		field, value,
		// Set OID field to have ability to identify AII if the object will be deleted
		dbms.AIIFieldOID, objKey.Host + `:` + objKey.Path,
	)

	// Handle error
	if err := res.Err(); err != nil {
		return fmt.Errorf("(RedisCli:setAIIField) HSET for key %s (%s = %s) returned error: %w",
			RedisAIIPrefix + id, field, value, err)
	}

	// OK
	return nil
}

func (rc *RedisClient) deleteAII(args *dbms.AIIArgs, ids types.IdKeyMap) (int64, int64, error) {
	var err error

	td := int64(0)	// Tags deleted
	dd := int64(0)	// Descriptions deleted

	// Delete tags if requested
	if args.Tags != nil {
		// Check for first tags for ALL value
		if args.Tags[0] == dbms.AIIAllTags {
			// Need to clear all tags
			td, err = rc.clearAIIField(dbms.AIIFieldTags, ids.Keys())
		} else {
			// Need to remove the separate tags
			td, err = rc.delTags(args.Tags, ids)
		}

		if err != nil {
			return td, dd, fmt.Errorf("(RedisCli:deleteAII) %w", err)
		}
	}

	// Delete description if requested
	if args.Descr == dbms.AIIDelDescr {
		// Clear description for selected identifiers
		dd, err = rc.clearAIIField(dbms.AIIFieldDescr, ids.Keys())
		if err != nil {
			return td, dd, fmt.Errorf("(RedisCli:deleteAII) %w", err)
		}
	}

	// OK
	return td, dd, nil
}

func (rc *RedisClient) delTags(tags []string, ids types.IdKeyMap) (int64, error) {
	// Convert list of tags to map to check existing tags for need to be deleted
	toDelTags := tools.NewStrSet(tags...)

	tu := int64(0) // Total changed values of tags fields

	// Set of AII when tags field should be cleared
	clearTags := tools.NewStrSet()

	log.D("(RedisCli:delTags) Collecting AII existing tags")
	// Do for each identifier
	for id, objKey := range ids {
		// Make a key
		key := RedisAIIPrefix + id

		// Get list of keys of this hash
		aiiTagsStr, err := rc.c.HGet(rc.Ctx, key, dbms.AIIFieldTags).Result()
		if err != nil {
			if errors.Is(err, RedisNotFound) {
				// No tags field there, skip
				continue
			}
			return tu, fmt.Errorf("(RedisCli:delTags) cannot get value %q field of %q: %w", dbms.AIIFieldTags, key, err)
		}

		// Create set of tags which need to keep
		keepTags := tools.NewStrSet(
			// Split string by comma to create a list of tags
			strings.Split(aiiTagsStr, ",")...).
			// Remove toDelTags from full set of tags belonging to id
			Del(toDelTags.List()...)

		// Check for nothing to keep
		if keepTags.Empty() {
			// All tags should be removed from this item, add to queue to deletion
			clearTags.Add(id)
			// Now continue with the next id
			continue
		}

		// Compare value of keepTags and existing tags
		if strings.Join(keepTags.List(), ",") == aiiTagsStr {
			// No tags should be removed from this item, skip
			log.D("(RedisCli:delTags) No tags update required for %s", id)
			continue
		}

		// Need to set new value of the tags field without removed tags
		n, err := rc.setTags(keepTags.List(), types.IdKeyMap{id: objKey})
		if err != nil {
			return tu, fmt.Errorf("(RedisCli:delTags) cannot remove tags %v from %q: %w", tags, id, err)
		}

		tu += n
	}

	// Check for AII from which need to remove the tags field
	if !clearTags.Empty() {
		// Call clear tags for this AII
		n, err := rc.clearAIIField(dbms.AIIFieldTags, clearTags.List())
		if err != nil {
			return tu, fmt.Errorf("(RedisCli:delTags) cannot clear tags: %w", err)
		}

		tu += n
	}

	// OK
	return tu, nil
}

func (rc *RedisClient) clearAIIField(field string, ids []string) (int64, error) {
	// Prepare sets of identifiers that should be cleared
	toDelKey, toDelField, idxRm, err := rc.prepClearAIISets(field, ids)
	if err != nil {
		return 0, fmt.Errorf("(RedisCli:clearAIIField) cannot prepare datasets to clear: %w", err)
	}

	// Check for nothing to delete
	if len(toDelKey) == 0 && len(toDelField) == 0 {
		log.D("(RedisCli:clearAIIField) Field %q are not set for these objects", field)
		// OK
		return 0, nil
	}

	// Total cleared
	tc := int64(0)

	// Check for keys to delete
	if len(toDelKey) != 0 {
		log.D("(RedisCli:clearAIIField) AII will be deleted because there are no valuable fields than %q: %v", field, toDelKey)

		res := rc.c.Del(rc.Ctx, toDelKey...)
		if res.Err() != nil {
			return tc, fmt.Errorf("(RedisCli:clearAIIField) cannot delete AII keys %v: %v", toDelKey, res.Err())
		}

		tc += res.Val()

	}

	if len(toDelField) != 0 {
		log.D("(RedisCli:clearAIIField) The field %q will be removed from AII: %v", field, toDelField)
		// Delete the cleared field from each entry
		for _, key := range toDelField {
			n, err := rc.c.HDel(rc.Ctx, key, field).Result()
			if err != nil {
				return tc, fmt.Errorf("cannot remove field %q from key %q: %w", field, key, err)
			}
			tc += n
		}
	}

	// Remove objects from index with objects that use field
	idxSet := RedisAIIDSetPrefix + field
	if n, err := rc.c.SRem(rc.Ctx, idxSet, idxRm...).Result(); err != nil {
		log.E("(RedisCli:clearAIIField) cannot remove identifiers %v from set %q: %v - " +
				"search results may be incorrect", idxRm, idxSet, err)
	} else {
		log.D("(RedisCli:clearAIIField) Removed %d members from set %q", n, idxSet)
	}

	// OK
	return tc, nil
}

func (rc *RedisClient) prepClearAIISets(field string, ids[]string) (toDelKey, toDelField []string, idxRm []any, err error) {
	// List of keys that can be safely deleted to clearing field
	toDelKey = make([]string, 0, len(ids))
	// List of keys on which only the field should be deleted
	toDelField = make([]string, 0, len(ids))

	// List of identifiers to remove from index
	idxRm = make([]any, 0, len(ids))

	log.D("(RedisCli:prepClearAIISets) Collecting AII info to clearing field %q...", field)
	// Do for each identifier
	for _, id := range ids {
		// Make a key
		key := RedisAIIPrefix + id

		// Get list of keys of this hash
		keys, err := rc.c.HKeys(rc.Ctx, key).Result()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("cannot get number of keys for %q: %w", key, err)
		}

		// Flag the presence of fields that are not OIDs and are not in the field set for cleaning
		other := false
		ff := false	// Field found
		for _, f := range keys {
			// Check for field name selected from DB (f) is cleared field name
			if field == f {
				ff = true
			} else if f != dbms.AIIFieldOID {
				// Selected name f is not cleared field name
				// and not OID field name - this is some other field
				other = true
			}
		}

		// Check for cleared field was not found
		if !ff {
			// Skip this ID
			continue
		}

		// Check number of other fields
		if other {
			// Other fields found, need to delete only the cleared field
			toDelField = append(toDelField, key)
		} else {
			// This key can be deleted because does not contain something other
			// that OID and the cleared field which has to be deleted
			toDelKey = append(toDelKey, key)
		}

		// Add id to list of identifiers to remove from index
		idxRm = append(idxRm, id)
	}

	// OK
	return toDelKey, toDelField, idxRm, nil
}
