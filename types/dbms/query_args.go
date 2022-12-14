package dbms

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/r-che/dfi/common/parse"
	"github.com/r-che/dfi/types"
)

// Separator between start/end of some range passed by command line
const dataRangeSep = ".."

type QueryArgs struct {
	// Search phrases
	SP			[]string

	// Mtime related
	MtimeStart	int64
	MtimeEnd	int64
	MtimeSet	[]int64

	// Size related
	SizeStart	int64
	SizeEnd		int64
	SizeSet		[]int64

	Types		[]string
	CSums		[]string
	Ids			[]string
	Hosts		[]string
	AIIFields	[]string

	types.SearchFlags
	types.CommonFlags
}

func NewQueryArgs() *QueryArgs {
	return &QueryArgs{}
}

func (qa *QueryArgs) SetSearchPhrases(searchPhrases []string) *QueryArgs {
	// Trim possible leading and trailing white spaces
	sp := make([]string, 0, len(searchPhrases))
	for _, s := range searchPhrases {
		sp = append(sp, strings.TrimSpace(s))
	}

	qa.SP = sp

	return qa
}

func (qa *QueryArgs) Clone() *QueryArgs {
	rv := *qa

	rv.SP = make([]string, len(qa.SP))
	copy(rv.SP, qa.SP)

	rv.MtimeSet = make([]int64, len(qa.MtimeSet))
	copy(rv.MtimeSet, qa.MtimeSet)
	rv.SizeSet = make([]int64, len(qa.SizeSet))
	copy(rv.SizeSet, qa.SizeSet)

	rv.Types = make([]string, len(qa.Types))
	copy(rv.Types, qa.Types)

	rv.CSums = make([]string, len(qa.CSums))
	copy(rv.CSums, qa.CSums)

	rv.Ids = make([]string, len(qa.Ids))
	copy(rv.Ids, qa.Ids)

	rv.Hosts = make([]string, len(qa.Hosts))
	copy(rv.Hosts, qa.Hosts)

	rv.AIIFields = make([]string, len(qa.AIIFields))
	copy(rv.AIIFields, qa.AIIFields)

	return &rv
}

func (qa *QueryArgs) IsMtime() bool {
	return len(qa.MtimeSet) != 0 || qa.MtimeStart != 0 || qa.MtimeEnd != 0
}

func (qa *QueryArgs) IsSize() bool {
	return len(qa.SizeSet) != 0 || qa.SizeStart != 0 || qa.SizeEnd != 0
}

func (qa *QueryArgs) IsType() bool {
	return len(qa.Types) != 0
}

func (qa *QueryArgs) IsChecksum() bool {
	return len(qa.CSums) != 0
}

func (qa *QueryArgs) IsAIIFields() bool {
	return len(qa.AIIFields) != 0
}

func (qa *QueryArgs) IsHost() bool {
	return len(qa.Hosts) != 0
}

func (qa *QueryArgs) IsIds() bool {
	return len(qa.Ids) != 0
}

func (qa *QueryArgs) OnlyAII() bool {
	return qa.OnlyTags || qa.OnlyDescr
}

func (qa *QueryArgs) CanSearch(searchPhrases []string) bool {
	// Check for any search phrases
	for _, sp := range searchPhrases {
		if sp != "" {
			// Non-empty search phrase will be sufficient
			return true
		}
	}

	if qa.IsMtime() || qa.IsSize() || qa.IsType() ||
	   qa.IsChecksum() || qa.IsHost() || qa.IsAIIFields() {
		// Sufficient conditions to search query
		return true
	}

	// Insufficient
	return false
}

func (qa *QueryArgs) UseAII() bool {
	return qa.UseTags || qa.UseDescr
}

func (qa *QueryArgs) ParseMtimes(mtimeLine string) error {
	// Possible variants:
	// * ts1[,ts2,ts3...]
	// * ts1..ts2
	// * ts1..
	// * ..ts2

	// Need to determine format - set or range?
	if strings.Contains(mtimeLine, dataRangeSep) {
		// Range provided
		return qa.parseMtimesRange(mtimeLine)
	}

	// Set of times provided, parse it and return
	return qa.parseMtimeSet(mtimeLine)
}

//nolint:cyclop	// Simplifying the code will not make it clearer
func (qa *QueryArgs) parseMtimesRange(mtimeRange string) error {
	// Split to check correctness
	tsRange := strings.Split(mtimeRange, dataRangeSep)
	//nolint:gomnd	// Check for range length - it always should be == 2
	if len(tsRange) != 2 {
		return fmt.Errorf("invalid mtime range %q", mtimeRange)
	}

	start, end := tsRange[0], tsRange[1]

	var err error
	// Need to select correct case of ranges
	switch {
	// Start and end both set
	case start != "" && end != "":
		if qa.MtimeStart, err = parseTime(start); err != nil {
			return fmt.Errorf("invalid mtime range start in %q: %w", mtimeRange, err)
		}
		if qa.MtimeEnd, err = parseTime(end); err != nil {
			return fmt.Errorf("invalid mtime range end in %q: %w", mtimeRange, err)
		}

		// Check that start < end
		if qa.MtimeEnd <= qa.MtimeStart {
			return fmt.Errorf("invalid mtime range %q - end of the range must be greater than start", mtimeRange)
		}

	// Only start is set
	case start != "" && end == "":
		if qa.MtimeStart, err = parseTime(start); err != nil {
			return fmt.Errorf("invalid mtime range start in %q: %w", mtimeRange, err)
		}

	// Only end is set
	case start == "" && end != "":
		if qa.MtimeEnd, err = parseTime(end); err != nil {
			return fmt.Errorf("invalid mtime range end in %q: %w", mtimeRange, err)
		}

	default:
		return fmt.Errorf("invalid mtime range %q", mtimeRange)
	}

	// OK, range parsed successfully
	return nil
}

func (qa *QueryArgs) parseMtimeSet(mtimeSet string) error {
	// Replace each escaped comma by fake delimiter to avoid wrong split of TS containing a comma
	const fakeDelim = "\u0000|\u0000"
	mtimeSet = strings.ReplaceAll(mtimeSet, `\,`, fakeDelim)

	// Split set and parse one by one
	qa.MtimeSet = []int64{}
	for _, timeStr := range strings.Split(mtimeSet, ",") {
		ts, err := parseTime(
			// Return commas back if they were replaced by escape character
			strings.ReplaceAll(timeStr, fakeDelim, ","))
		if err != nil {
			return fmt.Errorf("invalid mtimes set: %w", err)
		}

		// Append parsed TS
		qa.MtimeSet = append(qa.MtimeSet, ts)
	}

	// OK
	return nil
}

// Supported formats of timestamps
func TSFormats() []string {
	return []string{
		// "01/02 03:04:05PM '06 -0700" // The reference time, in numerical order.

		// Custom short formats
		"2006.01.02",
		"2006.01.02 MST",
		"2006-01-02",
		"2006-01-02 MST",
		"2006/01/02",
		"2006/01/02 MST",
		"2006.01.02 15:04",
		"2006.01.02 15:04 MST",
		"2006-01-02 15:04",
		"2006-01-02 15:04 MST",
		"2006/01/02 15:04",
		"2006/01/02 15:04 MST",
		"2006.01.02 15:04:05",
		"2006.01.02 15:04:05 MST",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05 MST",
		"2006/01/02 15:04:05",
		"2006/01/02 15:04:05 MST",

		// Default linux date output
		"Mon 02 Jan 2006 15:04:05 PM MST",

		// See standard list there: https://pkg.go.dev/time#pkg-constants
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		time.RFC822,
		time.RFC822Z,
		time.RFC850,
		time.RFC1123,
		time.RFC1123Z,
		time.RFC3339,
		time.RFC3339Nano,
		time.Kitchen,
		time.Stamp,
		time.StampMilli,
		time.StampMicro,
		time.StampNano,
	}
}

func parseTime(timeStr string) (int64, error) {
	// Try to convert ts as unix timestamp
	if ts, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		// Ok, return timestamp
		return ts, nil
	}

	// Try to convert as string representations
	for _, format := range TSFormats() {
		if ts, err := time.Parse(format, timeStr); err == nil {
			// OK, parsed
			return ts.Unix(), nil
		}
	}

	return -1, fmt.Errorf("cannot parse time %q", timeStr)
}

func (qa *QueryArgs) ParseSizes(sizeLine string) error {
	// Possible variants:
	// * size1[,size2,size3...]
	// * size1..size2
	// * size1..
	// * ..size2

	// Need to determine format - set or range?
	if strings.Contains(sizeLine, dataRangeSep) {
		// Sizes rane provided
		return qa.parseSizesRange(sizeLine)
	}

	// Set of sizes provided

	// Split set and parse one by one
	qa.SizeSet = []int64{}
	for _, sizeStr := range strings.Split(sizeLine, ",") {
		ts, err := parseSize(sizeStr)
		if err != nil {
			return fmt.Errorf("invalid sizes set %q: %w", sizeLine, err)
		}

		// Append parsed TS
		qa.SizeSet = append(qa.SizeSet, ts)
	}

	// OK, set parsed successfully
	return nil
}

//nolint:cyclop	// Simplifying the code will not make it clearer
func (qa *QueryArgs) parseSizesRange(sizeLine string) error {
	// It should be a range, split to check
	sizeRange := strings.Split(sizeLine, dataRangeSep)
	//nolint:gomnd	// Check for range length - it always should be == 2
	if len(sizeRange) != 2 {
		return fmt.Errorf("invalid mtime range %q", sizeLine)
	}

	start, end := sizeRange[0], sizeRange[1]

	var err error
	// Need to select correct case of ranges
	switch {
	// Start and end both set
	case start != "" && end != "":
		if qa.SizeStart, err = parseSize(start); err != nil {
			return fmt.Errorf("invalid size range start in %q: %w", sizeLine, err)
		}
		if qa.SizeEnd, err = parseSize(end); err != nil {
			return fmt.Errorf("invalid size range end in %q: %w", sizeLine, err)
		}
		// Check that start < end
		if qa.SizeEnd <= qa.SizeStart {
			return fmt.Errorf("invalid size range %q - end of the range must be greater than start", sizeLine)
		}
	// Only start is set
	case start != "" && end == "":
		if qa.SizeStart, err = parseSize(start); err != nil {
			return fmt.Errorf("invalid size range start in %q: %w", sizeLine, err)
		}
	// Only end is set
	case start == "" && end != "":
		if qa.SizeEnd, err = parseSize(end); err != nil {
			return fmt.Errorf("invalid size range end in %q: %w", sizeLine, err)
		}
	default:
		return fmt.Errorf("invalid size range %q", sizeLine)
	}

	// OK, range parsed successfully
	return nil
}

const (
	K = 1024
	M = K * 1024
	G = M * 1024
	T = G * 1024
	P = T * 1024
	E = P * 1024
)
var sizeSuffixes = map[string]int64 {
	"k": K,
	"m": M,
	"g": G,
	"t": T,
	"p": P,
	"e": E,
}
func parseSize(sizeStr string) (int64, error) {
	// Convert size string to lower case to ignore case of possible suffix
	sizeStr = strings.ToLower(sizeStr)

	// Multiplier for unit suffix
	multiplier := int64(1)

	// Check for size string has unit suffix
	for suf, mult := range sizeSuffixes {
		if strings.HasSuffix(sizeStr, suf) {
			// Assign correct multiplier
			multiplier = mult
			// Remove suffix letter from end of size string
			sizeStr = sizeStr[:len(sizeStr)-1]
			break
		}
	}

	// Convert string size to int representation
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("invalid size value %q", sizeStr)
	}

	return size * multiplier, nil
}

func (qa *QueryArgs) ParseTypes(val string, allowed []string) error {
	return parse.StringsSet(&qa.Types, "type", val, allowed...)
}

func (qa *QueryArgs) ParseSums(val string) error {
	return parse.StringsSet(&qa.CSums, "checksum", val)
}

func (qa *QueryArgs) ParseHosts(val string) error {
	return parse.StringsSet(&qa.Hosts, "host", val)
}

func (qa *QueryArgs) ParseAIIFields(val string, allowed []string) error {
	return parse.StringsSet(&qa.AIIFields, "field name", val, allowed...)
}

func (qa *QueryArgs) AddIds(ids ...string) *QueryArgs {
	qa.Ids = append(qa.Ids, ids...)
	return qa
}

func (qa *QueryArgs) AddChecksums(csums ...string) *QueryArgs {
	qa.CSums = append(qa.CSums, csums...)
	return qa
}
