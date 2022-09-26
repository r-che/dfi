package dbi

import (
	"fmt"
	"strings"
	"strconv"
	"time"

)

// Separator between start/end of some range passed by command line
const dataRangeSep = ".."

type QueryArgs struct {
	// Search phrases
	sp			[]string

	// Mtime related
	mtimeStart	int64
	mtimeEnd	int64
	mtimeSet	[]int64

	// Size related
	sizeStart	int64
	sizeEnd		int64
	sizeSet		[]int64

	types		[]string
	csums		[]string
	ids			[]string
	hosts		[]string

	orExpr		bool
	negExpr		bool
	onlyName	bool
	deep		bool
}

func NewQueryArgs(searchPhrases []string) *QueryArgs {
	return &QueryArgs{
		sp: searchPhrases,
	}
}

func (qa *QueryArgs) Clone() *QueryArgs {
	rv := *qa

	rv.sp = make([]string, len(qa.sp))
	copy(rv.sp, qa.sp)

	rv.mtimeSet = make([]int64, len(qa.mtimeSet))
	copy(rv.mtimeSet, qa.mtimeSet)
	rv.sizeSet = make([]int64, len(qa.sizeSet))
	copy(rv.sizeSet, qa.sizeSet)

	rv.types = make([]string, len(qa.types))
	copy(rv.types, qa.types)

	rv.csums = make([]string, len(qa.csums))
	copy(rv.csums, qa.csums)

	rv.ids = make([]string, len(qa.ids))
	copy(rv.ids, qa.ids)

	rv.hosts = make([]string, len(qa.hosts))
	copy(rv.hosts, qa.hosts)

	return &rv
}

func (qa *QueryArgs) isMtime() bool {
	return len(qa.mtimeSet) != 0 || qa.mtimeStart != 0 || qa.mtimeEnd != 0
}

func (qa *QueryArgs) isSize() bool {
	return len(qa.sizeSet) != 0 || qa.sizeStart != 0 || qa.sizeEnd != 0
}

func (qa *QueryArgs) isType() bool {
	return len(qa.types) != 0
}

func (qa *QueryArgs) isChecksum() bool {
	return len(qa.csums) != 0
}

func (qa *QueryArgs) isID() bool {
	return len(qa.ids) != 0
}

func (qa *QueryArgs) isHost() bool {
	return len(qa.hosts) != 0
}

func (qa *QueryArgs) CanSearch(searchPhrases []string) bool {
	// Check for any search phrases
	for _, sp := range searchPhrases {
		if sp != "" {
			// Non-empty search phrase will be sufficient
			return true
		}
	}

	if qa.isMtime() || qa.isSize() || qa.isType() || qa.isChecksum() || qa.isID() || qa.isHost() {
		// Sufficient conditions to search query
		return true
	}

	// Insufficient
	return false
}

func (qa *QueryArgs) ParseMtimes(mtimeLine string) error {
	// Possible variants:
	// * ts1[,ts2,ts3...]
	// * ts1..ts2
	// * ts1..
	// * ..ts2

	// Need to determine format - set or range?
	if strings.Index(mtimeLine, dataRangeSep) != -1 {
		// It should be a range, split to check
		tsRange := strings.Split(mtimeLine, dataRangeSep)
		// Check for range length - it always should be == 2
		if len(tsRange) != 2 {
			return fmt.Errorf("invalid mtime range %q", mtimeLine)
		}

		var err error
		// Need to select correct case of ranges
		switch {
			// Start and end both set
			case tsRange[0] != "" && tsRange[1] != "":
				if qa.mtimeStart, err = parseTime(tsRange[0]); err != nil {
					return fmt.Errorf("invalid mtime range start in %q: %v", mtimeLine, err)
				}
				if qa.mtimeEnd, err = parseTime(tsRange[1]); err != nil {
					return fmt.Errorf("invalid mtime range end in %q: %v", mtimeLine, err)
				}
				// Check that start < end
				if qa.mtimeEnd <= qa.mtimeStart {
					return fmt.Errorf("invalid mtime range %q - end of the range must be greater than start", mtimeLine)
				}
			// Only start is set
			case tsRange[0] != "" && tsRange[1] == "":
				if qa.mtimeStart, err = parseTime(tsRange[0]); err != nil {
					return fmt.Errorf("invalid mtime range start in %q: %v", mtimeLine, err)
				}
			// Only end is set
			case tsRange[0] == "" && tsRange[1] != "":
				if qa.mtimeEnd, err = parseTime(tsRange[1]); err != nil {
					return fmt.Errorf("invalid mtime range end in %q: %v", mtimeLine, err)
				}
			default:
				return fmt.Errorf("invalid mtime range %q", mtimeLine)
		}

		// OK, range parsed successfuly
		return nil
	}

	// Set of times provided

	// Split set and parse one by one
	qa.mtimeSet = []int64{}
	for _, timeStr := range strings.Split(mtimeLine, ",") {
		ts, err := parseTime(timeStr)
		if err != nil {
			return fmt.Errorf("invalid mtimes set %q: %v", mtimeLine, err)
		}

		// Append parsed TS
		qa.mtimeSet = append(qa.mtimeSet, ts)
	}

	// OK
	return nil
}

// Supported formats of timestamps
var tsFormats = []string {
	// "01/02 03:04:05PM '06 -0700" // The reference time, in numerical order.

	// Custom short formats
	"2006.01.02",
	"2006-01-02",
	"2006/01/02",
	"2006.01.02 15:04",
	"2006-01-02 15:04",
	"2006/01/02 15:04",
	"2006.01.02 15:04:05",
	"2006-01-02 15:04:05",
	"2006/01/02 15:04:05",

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
func parseTime(timeStr string) (int64, error) {
	// Try to convert ts as unix timestamp
	if ts, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		// Ok, return timestamp
		return ts, nil
	}

	// Try to convert as string representations
	for _, format := range tsFormats {
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
	if strings.Index(sizeLine, dataRangeSep) != -1 {
		// It should be a range, split to check
		sizeRange := strings.Split(sizeLine, dataRangeSep)
		// Check for range length - it always should be == 2
		if len(sizeRange) != 2 {
			return fmt.Errorf("invalid mtime range %q", sizeLine)
		}

		var err error
		// Need to select correct case of ranges
		switch {
			// Start and end both set
			case sizeRange[0] != "" && sizeRange[1] != "":
				if qa.sizeStart, err = parseSize(sizeRange[0]); err != nil {
					return fmt.Errorf("invalid size range start in %q: %v", sizeLine, err)
				}
				if qa.sizeEnd, err = parseSize(sizeRange[1]); err != nil {
					return fmt.Errorf("invalid size range end in %q: %v", sizeLine, err)
				}
				// Check that start < end
				if qa.sizeEnd <= qa.sizeStart {
					return fmt.Errorf("invalid size range %q - end of the range must be greater than start", sizeLine)
				}
			// Only start is set
			case sizeRange[0] != "" && sizeRange[1] == "":
				if qa.sizeStart, err = parseSize(sizeRange[0]); err != nil {
					return fmt.Errorf("invalid size range start in %q: %v", sizeLine, err)
				}
			// Only end is set
			case sizeRange[0] == "" && sizeRange[1] != "":
				if qa.sizeEnd, err = parseSize(sizeRange[1]); err != nil {
					return fmt.Errorf("invalid size range end in %q: %v", sizeLine, err)
				}
			default:
				return fmt.Errorf("invalid size range %q", sizeLine)
		}

		// OK, range parsed successfuly
		return nil
	}

	// Set of sizes provided

	// Split set and parse one by one
	qa.sizeSet = []int64{}
	for _, sizeStr := range strings.Split(sizeLine, ",") {
		ts, err := parseSize(sizeStr)
		if err != nil {
			return fmt.Errorf("invalid sizes set %q: %v", sizeLine, err)
		}

		// Append parsed TS
		qa.sizeSet = append(qa.sizeSet, ts)
	}

	// OK, set parsed successfuly
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

func (qa *QueryArgs) ParseTypes(typesLine string, allowed []string) error {
	qa.types = []string{}

	argTypes:
	for _, argType := range strings.Split(typesLine, ",") {
		for _, kt := range allowed {
			if argType == kt {
				qa.types = append(qa.types, argType)
				continue argTypes
			}
		}

		return fmt.Errorf("uknown type %q", argType)
	}

	return nil
}

func (qa *QueryArgs) ParseSums(cSumsLine string) error {
	qa.csums = strings.Split(cSumsLine, ",")

	for _, csum := range qa.csums {
		if csum == "" {
			return fmt.Errorf("empty checksum value in checksums line %q", cSumsLine)
		}
	}

	return nil
}

func (qa *QueryArgs) ParseIDs(idsLine string) error {
	qa.ids = strings.Split(idsLine, ",")

	for _, id := range qa.ids {
		if id == "" {
			return fmt.Errorf("empty ID value in ids line %q", idsLine)
		}
	}

	return nil
}

func (qa *QueryArgs) ParseHosts(hostsLine string) error {
	qa.hosts = strings.Split(hostsLine, ",")

	for _, host := range qa.hosts {
		if host == "" {
			return fmt.Errorf("empty host value in hosts line %q", hostsLine)
		}
	}

	return nil
}

func (qa *QueryArgs) SetNeg(neg bool) {
	qa.negExpr = neg
}

func (qa *QueryArgs) SetOnlyName(onlyName bool) {
	qa.onlyName = onlyName
}

func (qa *QueryArgs) SetOr(or bool) {
	qa.orExpr = or
}

func (qa *QueryArgs) SetDeep(deep bool) {
	qa.deep = deep
}
