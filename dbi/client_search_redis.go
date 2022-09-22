//go:build dbi_redis
package dbi

import (
	"fmt"
	"strings"

    rsh "github.com/RediSearch/redisearch-go/redisearch"
)

func (rc *RedisClient) Query(searchPhrases []string, qa *QueryArgs) ([]QueryResult, error) {
	// TODO
	// Common order:
	// * Add to query all additional arguments from query args
	// * For each search phrase:
	//   * Use SCAN MATCH
	//   * Try to use it as part of object name (using RediSearch)
	// * Build final query using or/not modifiers


	// Make redisearch query
	rshq := rshQuery(searchPhrases, qa)
	_=rshq


	if qa.isHost() {
		// TODO Results should be filtered by qa.hosts if provided
	}

	return nil, nil
}

func rshQuery(searchPhrases []string, qa *QueryArgs) string {
	// Query chunks
	chunks := []string{}

	if qa.isMtime() {
		chunks = append(chunks, prepMtime(qa))
	}
	if qa.isSize() {
		chunks = append(chunks, prepSize(qa))
	}
	if qa.isType() {
		chunks = append(chunks, prepType(qa))
	}
	if qa.isChecksum() {
		chunks = append(chunks, prepChecksum(qa))
	}
	if qa.isID() {
		chunks = append(chunks, prepID(qa))
	}

	// Make search phrases query - try to search them in found path and real path
	spEscaped := rsh.EscapeTextFileString(strings.Join(searchPhrases, ` | `))
	spQuery := fmt.Sprintf(`(@%s:"%s" | @%s:"%s")`, FieldFPath, spEscaped, FieldRPath, spEscaped)

	// Make a final query
	var argsQuery string
	negMark := ""
	if qa.negExpr {
		negMark = "-"
	}
	if qa.orExpr {
		argsQuery = fmt.Sprintf(`%s(%s)`, negMark, strings.Join(chunks, `|`))
	} else {
		argsQuery = fmt.Sprintf(`%s(%s)`, negMark, strings.Join(chunks, ` `))
	}

	// Return a final query
	return spQuery + ` ` + argsQuery
}

func prepMtime(qa *QueryArgs) string {
	return makeSetRangeQuery(FieldMTime, qa.mtimeStart, qa.mtimeEnd, qa.mtimeSet)
}

func prepSize(qa *QueryArgs) string {
	return makeSetRangeQuery(FieldSize, qa.sizeStart, qa.sizeEnd, qa.sizeSet)
}

func prepType(qa *QueryArgs) string {
	return `@` + FieldType + `:{` +  strings.Join(qa.types, `|`) + `}`
}

func prepChecksum(qa *QueryArgs) string {
	return `@` + FieldChecksum + `:{` +  strings.Join(qa.csums, `|`) + `}`
}

func prepID(qa *QueryArgs) string {
	return `@` + FieldID+ `:{` +  strings.Join(qa.ids, `|`) + `}`
}

func makeSetRangeQuery(field string, min, max int64, set []int64) string {
	// Is set is not provided
	if len(set) == 0 {
		// Then min/max query
		return fmt.Sprintf(`@%s:[%d %d]`, field, min, max)
	}

	// Build query from set of values
	chunks := make([]string, 0, len(set))

	for _, item := range set {
		chunks = append(chunks, fmt.Sprintf(`@%s:[%d %d]`, field, item, item))
	}

	return `(` + strings.Join(chunks, ` | `) + `)`
}
