package cfg

import (
	"fmt"
	"os"
)

var helpSubjs = map[string]string {
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

Certainly, you can use --or and --not options at the same time.`,

"ranges":	// TODO
`>>>> Condition ranges <<<<

"TODO RANGES HELP %[1]s!`,
}

func help(name, nameLong string, subjs []string) {
	// Help header
	fmt.Println("\n>>>>> " + nameLong + " help <<<<<")

	if len(subjs) == 0 {
		// Show all available help
		subjs = []string{
			`search`,
			`ranges`,
		}
	}

	nfSubjs := make([]string, 0, len(subjs))
	for _, subj := range subjs {
		text, ok := helpSubjs[subj]
		if !ok {
			nfSubjs = append(nfSubjs, subj)
			continue
		}
		fmt.Printf("\n" + text + "\n", name)
	}

	for _, subj := range nfSubjs {
		fmt.Printf("[WARNING!] No special help for %q\n", subj)
	}

	os.Exit(1)
}
