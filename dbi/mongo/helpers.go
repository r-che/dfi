package mongo

import (
	"fmt"
	"unicode/utf8"

	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/types/dbms"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	minNameTextMatchScore = 200	// empiric value
	scorePseudoFieldName = `__score__`
)

func pipelineConfVariadic(filter *Filter, pipeline mongo.Pipeline, configs []any) mongo.Pipeline {
	// Try to interpret each additional configuration parameter
	for _, conf := range configs {
		// Choose type of configuration
		switch conf := conf.(type) {
		case *dbms.QueryArgs:
			// Check for OnlyName is not set
			if !conf.OnlyName {
				// Currently, nothing to do
				continue
			}

			//
			// Need to add filter to the pipeline to match documents with very high search score
			// that can only have a document with the name field matched by full-text filter
			//

			// Need to check that filter uses full-text search
			if !filter.FullText() {
				// Skip, because OnlyName can be set only with the full text search
				continue
			}

			// Modify pipeline
			pipeline = append(pipeline,
				// Add pseudo-field to get search score
				bson.D{{`$addFields`, bson.D{{scorePseudoFieldName, bson.D{{`$meta`, `textScore`}}}}}},
				// Filter results by score
				bson.D{{`$match`, bson.D{{scorePseudoFieldName, bson.D{{`$gte`, minNameTextMatchScore}}}}}},
				// Remove pseudo-field from the output fields set
				bson.D{{`$addFields`, bson.D{{scorePseudoFieldName, `$REMOVE`}}}},
			)

		default:
			panic(fmt.Sprintf("unknown variadic configuration parameter for pipeline:" +
				" type - %T, value - %#v", conf, conf))
		}
	}

	// OK
	return pipeline
}

func mergeAII(args *dbms.AIIArgs, aii dbms.QRItem, doc *bson.D) (int64, int64, error) {
	// Tags/description updated counters for the current item
	var tu, du int64

	// Get list of tags which have to be set
	tags, err := mergeTags(args, aii)
	if err != nil {
		return 0, 0, fmt.Errorf("cannot add/set tags: %w", err)
	}

	if tags != nil {
		// Append tags field
		*doc = append(*doc, bson.E{`$set`, bson.D{{dbms.AIIFieldTags, tags}}})
		// Increase counter of tags updates
		tu++
	}

	// Add/set description
	descr, err := mergeDescr(args, aii)
	if err != nil {
		return 0, 0, fmt.Errorf("cannot add/set description: %w", err)
	}

	if descr != "" {
		// Append description
		*doc = append(*doc, bson.E{`$set`, bson.D{{dbms.AIIFieldDescr, descr}}})
		// Increase counter of description updates
		du++
	}

	return tu, du, nil
}

func mergeTags(args *dbms.AIIArgs, aii dbms.QRItem) ([]string, error) {
	// Check for any tags specified
	if args.Tags == nil {
		// Nothing to do
		return nil, nil
	}

	// Check for tags field exists
	tagsData, ok := aii[dbms.AIIFieldTags]
	if !ok {
		// Tags field does not exist, return argument as is
		return args.Tags, nil
	}

	//
	// Need to update existing tags value
	//

	// Check for correct type of tags
	tagsArr, ok := tagsData.(primitive.A)
	if !ok {
		return nil, fmt.Errorf("(MongoCli:mergeTags) AII item contains invalid field %q - type of field is %T," +
			" want primitive.A (array), item: %#v", dbms.AIIFieldTags, tagsData, aii)
	}

	// Make a set of existing tags
	tags := tools.NewSet[string]()
	// Check and convert each tag from loaded tags data, then add to resulting set
	for _, tagVal := range tagsArr {
		tag, ok := tagVal.(string)
		if !ok {
			// Skip this item
			return nil, fmt.Errorf("(MongoCli:mergeTags) AII item contains field %q with non-string item value: %#v",
				dbms.AIIFieldTags, aii)
		}

		// Add tag to the resulting set
		tags.Add(tag)
	}

	// Add to set tags provided by arguments, get the set complement
	// to determine was the tags field updated or not
	if addedTags := len(tags.AddComplement(args.Tags...)); addedTags == 0 {
		// No tags were added
		return nil, nil
	}

	// Return merged list of tags
	return tags.Sorted(), nil
}

func mergeDescr(args *dbms.AIIArgs, aii dbms.QRItem) (string, error) {
	// Check for description specified
	if args.Descr == "" {
		// Nothing to do
		return "", nil
	}

	// Check for description field does not exists
	descrData, ok := aii[dbms.AIIFieldDescr]
	if !ok {
		// Tags field does not exist, need to set argument as is
		return args.Descr, nil
	}

	// Check for correct type of description
	descr, ok := descrData.(string)
	if !ok {
		return "", fmt.Errorf("MongoCli:mergeDescr) AII item contains invalid field %q -" +
			" type of field is %T, want string, item: %#v", dbms.AIIFieldDescr, descrData, aii)
	}

	// Append new description value to the existing
	return descr + tools.Tern(args.NoNL, `; `, "\n") + args.Descr, nil
}

func primArrToStrList(val any) ([]string, error) {
	// Check for correct type of tags
	arr, ok := val.(primitive.A)
	if !ok {
		return nil, fmt.Errorf("(MongoCli:primArrToStrList) unexpected type %T of value %#v," +
			" expected - primitive.A", val, val)
	}

	// Make an output list
	out := make([]string, len(arr))

	// Convert all array values to strings
	for i, v := range arr {
		sv, ok := v.(string)
		if !ok {
			// Skip this item
			return nil, fmt.Errorf("(MongoCli:primArrToStrList) array %#v contains non-string item %#v (type: %T)",
				arr, v, v)
		}

		out[i] = sv
	}

	return out, nil
}

func validateUtf8Values(doc bson.D) error {
	for _, item := range doc {
		// Try to convert to string representation
		strVal, ok := item.Value.(string)
		// Skip all non-strings
		if !ok {
			continue
		}

		// Check
		if !utf8.Valid([]byte(strVal)) {
			return fmt.Errorf("(MongoCli:validateUtf8Values) field %q contains invalid utf8 sequence: %#v", item.Key, item.Value)
		}
	}

	// OK
	return nil
}

func fillAII(retFields []string, fields dbms.QRItem) (*dbms.AIIArgs, error) {
	// Create new AII structure
	aii := dbms.AIIArgs{}

	for _, field := range retFields {
		// Get field value
		v, ok := fields[field]
		if !ok {
			// Field was not found
			continue
		}

		// Choose correct type
		switch v := v.(type) {
		// Simple string value
		case string:
			if err := aii.SetFieldStr(field, v); err != nil {
				return nil, fmt.Errorf("cannot set field from result: %w", err)
			}
		// List (slice) of string values
		case primitive.A:
			// Convert to string list
			strList, err := primArrToStrList(v)
			if err != nil {
				return nil, fmt.Errorf("problem to handle field %q: %w",
					field, err)
			}
			if err := aii.SetFieldList(field, strList); err != nil {
				return nil, fmt.Errorf("cannot set field from result: %w", err)
			}
		// Unsupported type
		default:
			return nil, fmt.Errorf("field %q contains unsupported type %T of value %#v", field, v, v)
		}
	}

	return &aii, nil
}
