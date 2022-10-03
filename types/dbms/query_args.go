package dbms

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

	OrExpr		bool
	NegExpr		bool
	OnlyName	bool
	UseTags		bool
	OnlyTags	bool
	UseDescr	bool
	OnlyDescr	bool
	Deep		bool
}

func NewQueryArgs(searchPhrases []string) *QueryArgs {
	// Trim possible leading and trailing white spaces
	sp := make([]string, 0, len(searchPhrases))
	for _, s := range searchPhrases {
		sp = append(sp, strings.TrimSpace(s))
	}

	return &QueryArgs{
		SP: sp,
	}
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

	if qa.IsMtime() || qa.IsSize() || qa.IsType() || qa.IsChecksum() || qa.IsHost() {
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
				if qa.MtimeStart, err = parseTime(tsRange[0]); err != nil {
					return fmt.Errorf("invalid mtime range start in %q: %v", mtimeLine, err)
				}
				if qa.MtimeEnd, err = parseTime(tsRange[1]); err != nil {
					return fmt.Errorf("invalid mtime range end in %q: %v", mtimeLine, err)
				}
				// Check that start < end
				if qa.MtimeEnd <= qa.MtimeStart {
					return fmt.Errorf("invalid mtime range %q - end of the range must be greater than start", mtimeLine)
				}
			// Only start is set
			case tsRange[0] != "" && tsRange[1] == "":
				if qa.MtimeStart, err = parseTime(tsRange[0]); err != nil {
					return fmt.Errorf("invalid mtime range start in %q: %v", mtimeLine, err)
				}
			// Only end is set
			case tsRange[0] == "" && tsRange[1] != "":
				if qa.MtimeEnd, err = parseTime(tsRange[1]); err != nil {
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
	qa.MtimeSet = []int64{}
	for _, timeStr := range strings.Split(mtimeLine, ",") {
		ts, err := parseTime(timeStr)
		if err != nil {
			return fmt.Errorf("invalid mtimes set %q: %v", mtimeLine, err)
		}

		// Append parsed TS
		qa.MtimeSet = append(qa.MtimeSet, ts)
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
				if qa.SizeStart, err = parseSize(sizeRange[0]); err != nil {
					return fmt.Errorf("invalid size range start in %q: %v", sizeLine, err)
				}
				if qa.SizeEnd, err = parseSize(sizeRange[1]); err != nil {
					return fmt.Errorf("invalid size range end in %q: %v", sizeLine, err)
				}
				// Check that start < end
				if qa.SizeEnd <= qa.SizeStart {
					return fmt.Errorf("invalid size range %q - end of the range must be greater than start", sizeLine)
				}
			// Only start is set
			case sizeRange[0] != "" && sizeRange[1] == "":
				if qa.SizeStart, err = parseSize(sizeRange[0]); err != nil {
					return fmt.Errorf("invalid size range start in %q: %v", sizeLine, err)
				}
			// Only end is set
			case sizeRange[0] == "" && sizeRange[1] != "":
				if qa.SizeEnd, err = parseSize(sizeRange[1]); err != nil {
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
	qa.SizeSet = []int64{}
	for _, sizeStr := range strings.Split(sizeLine, ",") {
		ts, err := parseSize(sizeStr)
		if err != nil {
			return fmt.Errorf("invalid sizes set %q: %v", sizeLine, err)
		}

		// Append parsed TS
		qa.SizeSet = append(qa.SizeSet, ts)
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
	qa.Types = []string{}

	argTypes:
	for _, argType := range strings.Split(typesLine, ",") {
		for _, kt := range allowed {
			if argType == kt {
				qa.Types = append(qa.Types, argType)
				continue argTypes
			}
		}

		return fmt.Errorf("uknown type %q", argType)
	}

	return nil
}

func (qa *QueryArgs) ParseSums(CSumsLine string) error {
	qa.CSums = strings.Split(CSumsLine, ",")

	for _, csum := range qa.CSums {
		if csum == "" {
			return fmt.Errorf("empty checksum value in checksums line %q", CSumsLine)
		}
	}

	return nil
}

func (qa *QueryArgs) ParseHosts(hostsLine string) error {
	qa.Hosts = strings.Split(hostsLine, ",")

	for _, host := range qa.Hosts {
		if host == "" {
			return fmt.Errorf("empty host value in hosts line %q", hostsLine)
		}
	}

	return nil
}

func (qa *QueryArgs) SetIds(ids []string) {
	qa.Ids = ids
}

func (qa *QueryArgs) SetNeg(v bool) {
	qa.NegExpr = v
}

func (qa *QueryArgs) SetOnlyName(v bool) {
	qa.OnlyName = v
}

func (qa *QueryArgs) SetUseTags(v bool) {
	qa.UseTags = v
}

func (qa *QueryArgs) SetOnlyTags(v bool) {
	if v {
		qa.UseTags = true
	}
	qa.OnlyTags = v
}

func (qa *QueryArgs) SetUseDescr(v bool) {
	qa.UseDescr = v
}

func (qa *QueryArgs) SetOnlyDescr(v bool) {
	if v {
		qa.UseDescr = true
	}
	qa.OnlyDescr = v
}

func (qa *QueryArgs) SetOr(v bool) {
	qa.OrExpr = v
}

func (qa *QueryArgs) SetDeep(v bool) {
	qa.Deep = v
}
