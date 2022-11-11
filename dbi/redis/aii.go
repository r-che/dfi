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

	// Create map indentifiers found in DB
	fids := make(types.IdKeyMap, len(ids))
	for k, v := range qr {
		id, ok := v[dbms.FieldID]
		if !ok {
			log.E("(RedisCli:ModifyAII) Loaded invalid object from DB - no ID field (%q) was found: %s:%s", dbms.FieldID, k.Host, k.Path)
			continue
		}

		// Convert ID to string representation
		if sid, ok := id.(string); ok {
			fids[sid] = k
		} else {
			log.E("(RedisCli:ModifyAII) Loaded invalid object from DB - " +
				"ID field (%q) cannot be converted to string: %s:%s => %v", dbms.FieldID, k.Host, k.Path, id)
		}
	}

	// Check for all ids were found
	var nf []string
	for _, id := range ids {
		if _, ok := fids[id]; !ok {
			nf = append(nf, id)
		}
	}
	if nf != nil {
		return 0, 0, fmt.Errorf("(RedisCli:ModifyAII) the following identifiers do not exist in DB: %s", strings.Join(nf, " "))
	}

	// 2. Select modification operator

	switch op {
	case dbms.Update:
		return rc.updateAII(args, fids, add)
	case dbms.Delete:
		return rc.deleteAII(args, fids)
	default:
		panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}

	// Unreachable
	return -1, -1, nil
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

		// Make a list to make a set of new tags + existing tags
		allTags := make([]string, len(tags))
		copy(allTags, tags)

		// Load existing values of tags field
		tagsStr, err := rc.c.HGet(rc.Ctx, key, dbms.AIIFieldTags).Result()
		if err == nil {
			// Tags field extracted, make union between extracted existing tags and new tags
			allTags = append(allTags, strings.Split(tagsStr, ",")...)
		} else if errors.Is(err, RedisNotFound) {
			// Ok, currently no tags for this object, nothing to do
		} else {
			// Something went wrong
			return tu, fmt.Errorf("(RedisCli:addTags) cannot get tags field %q for key %q: %w", dbms.AIIFieldTags, key, err)
		}

		// Make unique sorted list of tags
		allTags = tools.NewStrSet(allTags...).List()

		// Compare existing tags and new set
		if tagsStr == strings.Join(allTags, ",") {
			// Skip update
			log.D("(RedisCli:addTags) No tags update required for %s", id)
			continue
		}

		// Set tags for the current identifier
		if _, err := rc.setTags(allTags, types.IdKeyMap{id: objKey}); err != nil {
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
	toDelTags := make(map[string]bool, len(tags))
	for _, tag := range tags {
		toDelTags[tag] = true
	}

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

		// Split string by set of tags
		aiiTags := strings.Split(aiiTagsStr, ",")

		// Select tags to keep
		keepTags := make([]string, 0, len(aiiTags))
		for _, tag := range aiiTags {
			if !toDelTags[tag] {
				keepTags = append(keepTags, tag)
			}
		}

		// Check for nothing to keep
		if len(keepTags) == 0 {
			// All tags should be removed from this item, add to queue to deletion
			clearTags.Add(id)
			// Now continue with the next id
			continue
		}

		// Compare value of keepTags and existing tags
		if strings.Join(keepTags, ",") == aiiTagsStr {
			// No tags should be removed from this item, skip
			log.D("(RedisCli:delTags) No tags update required for %s", id)
			continue
		}

		// Need to set new value of tags field value without removed tags
		n, err := rc.setTags(keepTags, types.IdKeyMap{id: objKey})
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
	// List of keys that can be safely deleted to clearing field
	toDelKey := make([]string, 0, len(ids))
	// List of keys on which only the field should be deleted
	toDelField := make([]string, 0, len(ids))

	// Total cleared
	tc := int64(0)

	// List of identifiers to remove from index
	idxRm := make([]any, 0, len(ids))

	log.D("(RedisCli:clearAIIField) Collecting AII info to clearing field %q...", field)
	// Do for each identifier
	for _, id := range ids {
		// Make a key
		key := RedisAIIPrefix + id

		// Get list of keys of this hash
		keys, err := rc.c.HKeys(rc.Ctx, key).Result()
		if err != nil {
			return tc, fmt.Errorf("RedisCli:clearAIIField) cannot get number of keys for %q: %w", key, err)
		}

		// Count number of fields that are not OID or cleared field
		nOther := 0
		ff := false	// Field found
		for _, f := range keys {
			// Check for field name selected from DB (f) is cleared field name
			if field == f {
				ff = true
			} else if f != dbms.AIIFieldOID {
				// Selected name f is not cleared field name
				// and not OID field name - this is some other field
				nOther++
			}
		}

		// Check for cleared field was not found
		if !ff {
			// Skip this ID
			continue
		}

		// Check number of other fields
		if nOther == 0 {
			// This key can be deleted because does not contain something other
			// that OID and the cleared field which has to be deleted
			toDelKey = append(toDelKey, key)
		} else {
			// Need to delete only the cleared field
			toDelField = append(toDelField, key)
		}

		// Add id to list of identifiers to remove from index
		idxRm = append(idxRm, id)
	}

	// Check for nothing to delete
	if len(toDelKey) == 0 && len(toDelField) == 0 {
		log.D("(RedisCli:clearAIIField) Field %q are not set for these objects", field)
		// OK
		return tc, nil
	}

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
