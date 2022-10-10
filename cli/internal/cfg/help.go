package cfg

import (
	"fmt"
	"os"
	"strings"

	"github.com/r-che/dfi/types/dbms"
)

var helpSubjs = map[string]string {
// Documentation about search
	"search":
`>>>> Search mode <<<<

Usage:

 $ %[1]s [search mode options...] [search phrases...]

>>> Search phrases <<<

By default search phrases (SP) used to match full path of objects. This means that
will be found objects that contain SP not only in the file name but also in the path.
E.g. we have files "/data/snow/ball.png" and "/data/snowball.png", SP contain
word "snow" - both files will be found. To use only name of files for search
use option --only-name, in this case only "/data/snow" will be found.

Special cases of using search phrases:
  * --descr - search phrases also matched to all existing descriptions of objects
  * --tags  - search phrases also matched to all existing tags assigned to objects
              Each search phrase treated as a single tag - any transformations, such
              as splitting into separate tags by commas, are NOT performed
  * --dupes - each search phrase treated ONLY as object ID, this means that
              ONLY the identifier field will be matched with the search phrases

In cases where objects have been found using the --descr, --tags or --dupes options,
they are treated as if they were found by search phrases.

>>> Search mode options <<<

The following options used as search conditions:
  --mtime ..., --size ..., --type ..., --checksum ..., --host ..., --aii-filled

Each of them can return a logical TRUE or FALSE.

By default, a set of conditions are joined with a logical AND, unless the
option --or is set, in that case them will be joined with a logical OR.

>>> Logical combining between search phrases and options in the search condition <<<

Each search phrase in the search condition is combined with other phrases using
logical OR. The resulting set of search phrases is combined with the set of
conditions created from options using logical AND:
 $ %[1]s --size 1G --mtime 2000.01.01 "keywords set#1" "keywords set#2"
Will be treated as:
  (path contains "keywords set#1" or "keywords set#2")
  AND (size == 1G AND mtime == 2000.01.01)

You can replace logical AND by logical OR between conditions created from options:
 $ %[1]s --or --size 1G --mtime 2000.01.01 "keywords set#1" "keywords set#2"
Will be treated as:
 (path contains "keywords set#1" or "keywords set#2")
 AND (size == 1G OR mtime == 2000.01.01)

You can inverse a summary result of search conditions created from options
using option --not:
 $ %[1]s --not --size 1G --mtime 2000.01.01 "keywords set#1" "keywords set#2"
Will be treated as:
 (path contains "keywords set#1" or "keywords set#2")
 AND NOT(size == 1G AND mtime == 2000.01.01)

Certainly, you can use --or and --not options at the same time.
`,

// Documentation about show
"show":
`>>>> Show mode <<<<

Usage:

 $ %[1]s --show OBJECT-ID1 OBJECT-ID2 ...

The --show mode shows information about objects with identifiers given
by command line arguments. The displayed information includes:

 * Object identifier
 * Host where the object was found
 * Absolute path to the object on the host
 * Type of the object, one of: ` + strings.Join(knownTypes, ", ")  + `
 * Size of the object in bytes
 * Modification time in human-readable and in Unix timestamp formats
 * Additional information if set:
   * Tags - comma-separated set of tags
   * Description - text description of the object, can be multiline

>>> Object identification <<<

To show information about objects, you need to identify these objects.
This can be performed this using the --show-ids (-i) option in the search mode, e.g:

 $ %[1]s --show-ids --size 1G
 5f04497f12286af5d709941e0c26ccee8467a9e4 storage-host:/data/backup/data1.bin
 a690808c0a02510f2fc1ec5e8aeb6ddec1f32b8f storage-host:/data/backup/data2.bin

The objects identifiers are displayed in the first column. Now, you can query
information about each of these objects using --show mode:

 $ %[1]s --show 5f04497f....8467a9e4 a690808c....c1f32b8f

To avoid copy of object identifiers manually, you can use the --show-only-ids (-I)
option in search mode, which only prints object identifiers:

 $ %[1]s --show $(%[1]s --show-only-ids --size 1G)

>>> Output modifiers <<<<

You can make the show mode output more machine-friendly by using the options:

  * --one-line (-o) - prints each information entry in a single line in simple
                      key:"value" format, separated by spaces
  * --json (-j) - prints information entries as a list of maps in JSON format,
                  if the option --one-line specified - JSON will be printed
				  in a single line

`,

// Documentation about values range
"range":
`>>>> Range of values <<<<

The --mtime and --size parameters take range of values as arguments.

General range format is:

  START..END

Where START and END are the correct values of a particular type, END value must
be greater than START, for example:

 $ %[1]s --size 5G..10G

Files with size between 5 and 10 gigabytes (including) will be found.

>>> Open ranges <<<

Using of open ranges are also allowed - either (but NOT both) ends of the range
can be omitted, in this case the range can be represented by the following forms:

  START.. - range includes values equal and greater START
  ..END   - range includes values lesser and equal END

For example:

 $ %[1]s --mtime ..1999.01.01

Files with modification time earlier than January 1, 1999 will be found

 $ %[1]s --size 10G..

Files greater than 10 gigabytes will be found.

Note: see "--docs times" to get information about supported timestamp formats.

>>> Single value <<<

A range can also be represented by a single value to match an object to a specific
value of the corresponding field:

  VALUE

For example:

 $ %[1]s --size 1G

Files with the exact size of 1 gigabyte will be found.
`,

// Documentation about timestamps
"timestamp":
`>>>> Supported timestamp formats <<<<

The option --mtime accepts a range (see "--docs ranges") bounded by timestamps.

Allowed timestamp formats are:

  * UNIX_TIMESTAMP - unsigned integer value
  * ` + strings.Join(dbms.TsFormats(), "\n  * ") + `

You can use different formats for the beginning and the end of the range:

 $ %[1]s --mtime "2000/01/01 UTC..946771200"

Files with modification time between Jan 01 and Jan 02, 2000 UTC will be found.
The value 946771200 is a Unix timestamp corresponding to January 02, 2000 UTC.

`,
}

func help(name, nameLong string, topics []string) {
	// If topics is empty
	if len(topics) == 0 {
		// Show all available topics
		topics = []string{
			// Modes
			`search`,
			`show`,
			// Values
			`range`,
			`timestamp`,
		}
	}

	// Help header
	fmt.Println("\n>>>>> " + nameLong + " documentation <<<<<\n")

	// Filter not existing topics
	nfTopics := make([]string, 0, len(topics))
	for i := 0; i < len(topics); {
		if _, ok := helpSubjs[topics[i]]; ok {
			i++
			continue
		}

		nfTopics = append(nfTopics, topics[i])
		// Remove unknown topic from list
		topics = append(topics[:i], topics[i+1:]...)
	}

	for i, topic := range topics {
		fmt.Printf(helpSubjs[topic] + "\n", name)
		if i != len(topics) - 1 {
			fmt.Println("-----")
		}
	}

	for _, subj := range nfTopics {
		fmt.Printf("[WARNING!] No special help for topic %q\n\n", subj)
	}

	os.Exit(1)
}
