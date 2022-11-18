package tools

import (
	"reflect"
	"sort"
	"testing"
)

// stp - "set test prameters"
type stp struct {
	empty	bool		// true if expected that created set is empty
	init	[]string	// initial value for NewSet
	add		[]string	// items that should be added
	del		[]string	// items that should be deleted
	want	[]string	// expected list of values
	sVal	string		// expected String() return value
}

func setTestsAdd() []*stp {
	return []*stp {
		&stp{	// Init empty
			empty:	true,
			add:	[]string{"val1", "val2", "val3"},
			want:	[]string{"val1", "val2", "val3"},
			sVal:	"(val1, val2, val3)",
		},
		&stp{	// Init non-empty
			init:	[]string{"val30", "val10", "val20"},
			add:	[]string{"val05", "val15", "val25"},
			want:	[]string{"val05", "val10", "val15", "val20", "val25", "val30"},
			sVal:	"(val05, val10, val15, val20, val25, val30)",
		},
		&stp{	// Init empty, add duplicates
			empty:	true,
			add:	[]string{"val3", "val2", "val3", "val1", "val2", "val1", "val0"},
			want:	[]string{"val0", "val1", "val2", "val3"},
			sVal:	"(val0, val1, val2, val3)",
		},
		&stp{	// Init non-empty, add duplicates
			init:	[]string{"val1", "val2", "val3"},
			add:	[]string{"val3", "val2", "val1", "val0"},
			want:	[]string{"val0", "val1", "val2", "val3"},
			sVal:	"(val0, val1, val2, val3)",
		},
	}
}

func setTestsDel() []*stp {
	return []*stp {
		&stp{	// No deletion
			init:	[]string{"val1", "val2", "val3"},
			want:	[]string{"val1", "val2", "val3"},
			sVal:	"(val1, val2, val3)",
		},
		&stp{	// Init + add, then delete something + non-existing
			init:	[]string{"val30", "val10", "val20"},
			add:	[]string{"val05", "val15", "val25"},
			del:	[]string{"val30", "val10", "NX-element", "val20"},
			want:	[]string{"val05", "val15", "val25"},
			sVal:	"(val05, val15, val25)",
		},
		&stp{	// Init empty, add duplicates, delete all + non-existing
			add:	[]string{"val3", "val2", "val3", "val1", "val2", "val1", "val0"},
			del:	[]string{"val0", "NX-element", "val1", "val2", "val3"},
			want:	[]string{},
			sVal:	"()",
		},
		&stp{	// Just more items for testing
			init:	[]string{
				"sldx", "dis", "xcf", "shx", "curl", "tam", "sdkm", "pgm", "ros", "pgn", "pcx",
				"msl", "sfd", "dna", "apr", "gim", "rdf-crypt", "cil", "sxd", "fxp", "xlf",
				"gpt", "sfd-hdstx", "dcm", "seed", "deb", "tau", "asics", "oxlicg", "ecig",
				"lxf", "mpm", "cbin", "sgl", "jrd", "hgl", "lzh", "inp", "slt", "cdx", "mmdb",
				"jpx", "odf", "msh", "avci", "cpl", "tamp", "sswf", "xpm", "xlsm", "hpub",
				"oth", "tgz", "ica", "lzx", "dpkg", "aif", "amr", "wtb", "txt", "ctx", "rsat",
				"tgf", "wadl", "rxn", "lhs", "wmlsc", "epub", "jxrs", "jph", "mpc", "atomsrv",
				"cmsc", "xls", "btif", "uis", "slc", "xltm", "meta4", "stf", "gqf", "movie",
				"teacher", "apkg", "lbe", "str", "PGB", "cea", "espass", "gau", "glb", "frm",
				"qfx", "uvh", "obj", "rld", "esf", "docm", "gen", "sxm", "iii", "mop",
			},
			del:	[]string{
				"ssvc", "gltf", "lgr", "quox", "rst", "sxi", "jsontd", "ait", "cdf", "uvh",
				"atomcat", "uoml", "semf", "sid", "pnm", "sgif", "dive", "stf", "dvi", "acu",
				"sjpg", "woff", "aal", "mpw", "wax", "lbe", "viv", "xltx", "igm", "sensmlc",
				"str", "sxd", "tam", "tamp", "tau", "teacher", "tgf", "tgz", "txt", "uis",
				"ttml", "jpx", "apng", "seml", "apk", "oth", "sdoc", "mid", "qam", "bib",
				"vsf", "frm", "wbxml", "ecelp7470", "lpf", "cdfx", "mcm", "iii", "cat", "cub",
				"dms", "swf", "sqlite", "gal", "nlu", "lbc", "gtm", "flx", "rld", "ppkg",
				"hgl", "hpub", "ica", "inp", "jph", "jrd", "jxrs", "lhs", "lxf", "lzh", "lzx",
				"sensml", "sxm", "grxml", "mmf", "cpl", "ogv", "uvf", "doc", "xcos", "anx",
				"xodt", "flw", "torrent", "boo", "man", "emb", "cmsc", "xps", "ico", "otp",
				"nsf", "edx", "tcu", "ngdat", "unityweb", "fzs", "ccc", "rsheet", "sdw",
				"meta4", "mmdb", "mop", "movie", "mpc", "mpm", "msh", "msl", "obj", "odf",
				"avci", "xct", "vtt", "azs", "esa", "tei", "atomsrv", "osm", "xlsb", "spp",
			},
			want:	[]string{
				"PGB", "aif", "amr", "apkg", "apr", "asics", "btif", "cbin", "cdx", "cea",
				"cil", "ctx", "curl", "dcm", "deb", "dis", "dna", "docm", "dpkg", "ecig",
				"epub", "esf", "espass", "fxp", "gau", "gen", "gim", "glb", "gpt", "gqf",
				"oxlicg", "pcx", "pgm", "pgn", "qfx", "rdf-crypt", "ros", "rsat", "rxn",
				"sdkm", "seed", "sfd", "sfd-hdstx", "sgl", "shx", "slc", "sldx", "slt", "sswf",
				"wadl", "wmlsc", "wtb", "xcf", "xlf", "xls", "xlsm", "xltm", "xpm",
			},
			sVal:	"(PGB, aif, amr, apkg, apr, asics, btif, cbin, cdx, cea, cil, ctx, curl, dcm," +
				" deb, dis, dna, docm, dpkg, ecig, epub, esf, espass, fxp, gau, gen, gim, glb," +
				" gpt, gqf, oxlicg, pcx, pgm, pgn, qfx, rdf-crypt, ros, rsat, rxn, sdkm, seed," +
				" sfd, sfd-hdstx, sgl, shx, slc, sldx, slt, sswf, wadl, wmlsc, wtb, xcf, xlf," +
				" xls, xlsm, xltm, xpm)",
			},
	}
}

func setTestsComplement() []*stp {
	return []*stp {
		&stp{
			init:	[]string{ "one", "two", "three" },
			add:	[]string{ "zero", "one", "ONE", "two", "TWO", "three" },
			want:	[]string{ "zero", "ONE", "TWO"},
		},
		&stp{
			init:	[]string{
				"webm", "csp", "gsheet", "sxm", "ddf", "mseq", "flw", "tnef", "smpg", "plf",
				"viv", "ice", "ogex", "sdkm", "ivp", "cdmio", "tsv", "oeb", "ssvc", "odi", "urim",
				"mpw", "axv", "xps", "rcprofile", "atomsvc", "nbp", "hal", "cod", "lxf", "dii",
				"xlsx", "mpn", "xop", "clkp", "tur", "azw3", "erf", "mmr", "sos", "ahead", "jtd",
				"vcx", "hps", "vcg", "lha", "VES", "heics", "tcl", "yin", "mol2", "plc", "kon",
				"iso", "swidtag", "epub", "gex", "svc", "umj", "xlam", "wlnk", "pls", "gan",
				"ptrom", "oxt", "ttml", "irp", "woff2", "ott", "vwx", "rfcxml", "gdl", "psd",
				"jng", "prf", "siv", "atfx", "rlc", "icf", "ogv", "ctx", "docm", "xul", "bmi", "pgp",
				"dcm", "bar", "pdf", "pkipath", "crw", "sxw", "nml", "bcpio", "msi", "cuc", "SAR",
				"xltm", "xer", "sgml", "sppt", "docx", "swf", "clkk", "pti", "val", "rst", "gtw",
			},
			add:	[]string{
				"viv", "ice", "ogex", "sdkm", "ivp", "cdmio", "tsv", "oeb", "ssvc", "odi", "urim",
				"viv", "ice", "ogex", "sdkm", "ivp", "cdmio", "tsv", "oeb", "ssvc", "odi", "urim",
				"stk", "rdp", "wasm", "cdxml", "ecigprofile", "mph", "fsc", "nnw", "ndc",
				"stk", "rdp", "wasm", "cdxml", "ecigprofile", "mph", "fsc", "nnw", "ndc",
				"xlsx", "mpn", "xop", "clkp", "tur", "azw3", "erf", "mmr", "sos", "ahead", "jtd",
				"xlsx", "mpn", "xop", "clkp", "tur", "azw3", "erf", "mmr", "sos", "ahead", "jtd",
				"bmed", "grxml", "kil", "scq", "package", "xodp", "png", "ngdat", "tat", "flv",
				"bmed", "grxml", "kil", "scq", "package", "xodp", "png", "ngdat", "tat", "flv",
				"iso", "swidtag", "epub", "gex", "svc", "umj", "xlam", "wlnk", "pls", "gan",
				"iso", "swidtag", "epub", "gex", "svc", "umj", "xlam", "wlnk", "pls", "gan",
				"dna", "aif", "ttl", "odc", "xhtml", "dart", "ggt", "vbk", "uvz", "gnumeric",
				"dna", "aif", "ttl", "odc", "xhtml", "dart", "ggt", "vbk", "uvz", "gnumeric",
				"jng", "prf", "siv", "atfx", "rlc", "icf", "ogv", "ctx", "docm", "xul", "bmi", "pgp",
				"jng", "prf", "siv", "atfx", "rlc", "icf", "ogv", "ctx", "docm", "xul", "bmi", "pgp",
				"ppd", "cii", "emm", "stif", "ptid", "clkx", "pwn", "asics", "ecelp4800",
				"ppd", "cii", "emm", "stif", "ptid", "clkx", "pwn", "asics", "ecelp4800",
				"xltm", "xer", "sgml", "sppt", "docx", "swf", "clkk", "pti", "val", "rst", "gtw",
				"xltm", "xer", "sgml", "sppt", "docx", "swf", "clkk", "pti", "val", "rst", "gtw",
			},
			want:	[]string{
				"stk", "rdp", "wasm", "cdxml", "ecigprofile", "mph", "fsc", "nnw", "ndc",
				"bmed", "grxml", "kil", "scq", "package", "xodp", "png", "ngdat", "tat", "flv",
				"dna", "aif", "ttl", "odc", "xhtml", "dart", "ggt", "vbk", "uvz", "gnumeric",
				"ppd", "cii", "emm", "stif", "ptid", "clkx", "pwn", "asics", "ecelp4800",
			},
		},
		&stp{
			init:	[]string{
				"viv", "ice", "ogex", "sdkm", "ivp", "cdmio", "tsv", "oeb", "ssvc", "odi", "urim",
				"stk", "rdp", "wasm", "cdxml", "ecigprofile", "mph", "fsc", "nnw", "ndc",
				"xlsx", "mpn", "xop", "clkp", "tur", "azw3", "erf", "mmr", "sos", "ahead", "jtd",
				"bmed", "grxml", "kil", "scq", "package", "xodp", "png", "ngdat", "tat", "flv",
				"iso", "swidtag", "epub", "gex", "svc", "umj", "xlam", "wlnk", "pls", "gan",
				"dna", "aif", "ttl", "odc", "xhtml", "dart", "ggt", "vbk", "uvz", "gnumeric",
				"jng", "prf", "siv", "atfx", "rlc", "icf", "ogv", "ctx", "docm", "xul", "bmi", "pgp",
				"ppd", "cii", "emm", "stif", "ptid", "clkx", "pwn", "asics", "ecelp4800",
				"xltm", "xer", "sgml", "sppt", "docx", "swf", "clkk", "pti", "val", "rst", "gtw",
			},
			add:	[]string{
				"webm", "csp", "gsheet", "sxm", "ddf", "mseq", "flw", "tnef", "smpg", "plf",
				"webm", "csp", "gsheet", "sxm", "ddf", "mseq", "flw", "tnef", "smpg", "plf",
				"viv", "ice", "ogex", "sdkm", "ivp", "cdmio", "tsv", "oeb", "ssvc", "odi", "urim",
				"viv", "ice", "ogex", "sdkm", "ivp", "cdmio", "tsv", "oeb", "ssvc", "odi", "urim",
				"mpw", "axv", "xps", "rcprofile", "atomsvc", "nbp", "hal", "cod", "lxf", "dii",
				"mpw", "axv", "xps", "rcprofile", "atomsvc", "nbp", "hal", "cod", "lxf", "dii",
				"xlsx", "mpn", "xop", "clkp", "tur", "azw3", "erf", "mmr", "sos", "ahead", "jtd",
				"xlsx", "mpn", "xop", "clkp", "tur", "azw3", "erf", "mmr", "sos", "ahead", "jtd",
				"vcx", "hps", "vcg", "lha", "VES", "heics", "tcl", "yin", "mol2", "plc", "kon",
				"vcx", "hps", "vcg", "lha", "VES", "heics", "tcl", "yin", "mol2", "plc", "kon",
				"iso", "swidtag", "epub", "gex", "svc", "umj", "xlam", "wlnk", "pls", "gan",
				"iso", "swidtag", "epub", "gex", "svc", "umj", "xlam", "wlnk", "pls", "gan",
				"ptrom", "oxt", "ttml", "irp", "woff2", "ott", "vwx", "rfcxml", "gdl", "psd",
				"ptrom", "oxt", "ttml", "irp", "woff2", "ott", "vwx", "rfcxml", "gdl", "psd",
				"jng", "prf", "siv", "atfx", "rlc", "icf", "ogv", "ctx", "docm", "xul", "bmi", "pgp",
				"jng", "prf", "siv", "atfx", "rlc", "icf", "ogv", "ctx", "docm", "xul", "bmi", "pgp",
				"dcm", "bar", "pdf", "pkipath", "crw", "sxw", "nml", "bcpio", "msi", "cuc", "SAR",
				"dcm", "bar", "pdf", "pkipath", "crw", "sxw", "nml", "bcpio", "msi", "cuc", "SAR",
				"xltm", "xer", "sgml", "sppt", "docx", "swf", "clkk", "pti", "val", "rst", "gtw",
				"xltm", "xer", "sgml", "sppt", "docx", "swf", "clkk", "pti", "val", "rst", "gtw",
			},
			want:	[]string{
				"webm", "csp", "gsheet", "sxm", "ddf", "mseq", "flw", "tnef", "smpg", "plf",
				"mpw", "axv", "xps", "rcprofile", "atomsvc", "nbp", "hal", "cod", "lxf", "dii",
				"vcx", "hps", "vcg", "lha", "VES", "heics", "tcl", "yin", "mol2", "plc", "kon",
				"ptrom", "oxt", "ttml", "irp", "woff2", "ott", "vwx", "rfcxml", "gdl", "psd",
				"dcm", "bar", "pdf", "pkipath", "crw", "sxw", "nml", "bcpio", "msi", "cuc", "SAR",
			},
		},
	}
}

func TestSetAdd(t *testing.T) {
	for testN, test := range setTestsAdd() {
		// Init new set
		s := NewSet(test.init...)

		// Test for empty
		if s.Empty() != test.empty {
			t.Errorf("[%d] method Empty returns %t, want - %t", testN, s.Empty(), test.empty)
			// Go to the next test
			continue
		}

		// Test for adding values
		s.Add(test.add...)

		// Test for produced list
		if l := s.Sorted(); !reflect.DeepEqual(l, test.want) {
			t.Errorf("[%d] method Sorted returned %#v, want - %#v", testN, l, test.want)
			// Go to the next test
			continue
		}

		// Test for produced string
		if str := s.String(); str != test.sVal {
			t.Errorf("[%d] method String returned %q, want - %q", testN, str, test.sVal)
			// Go to the next test
			continue
		}
	}
}

func TestSetDel(t *testing.T) {
	for testN, test := range setTestsDel() {
		// Init new set
		s := newSet(test)

		// Test for produced list
		if l := s.Sorted(); !reflect.DeepEqual(l, test.want) {
			t.Errorf("[%d] method Sorted returned %#v, want - %#v", testN, l, test.want)
			// Go to the next test
			continue
		}

		// Test for produced string
		if str := s.String(); str != test.sVal {
			t.Errorf("[%d] method String returned %q, want - %q", testN, str, test.sVal)
			// Go to the next test
			continue
		}
	}
}

func TestSetLen(t *testing.T) {
	for testN, test := range append(setTestsAdd(), setTestsDel()...) {
		// Init new set
		s := newSet(test)

		// Check for length
		if setLen := s.Len(); setLen != len(test.want) {
			t.Errorf("[%d] Set.Len() returned - %d, want - %d", testN, setLen, len(test.want))
		}
	}
}

func TestSetList(t *testing.T) {
	for testN, test := range append(setTestsAdd(), setTestsDel()...) {
		// Init new set
		s := newSet(test)

		// Get unsorted list
		l := s.List()

		// Make a copy to sort
		ls := make([]string, len(l))
		copy(ls, l)

		// Sort list before comparing
		sort.Strings(ls)

		// Compare
		if !reflect.DeepEqual(ls, test.want) {
			t.Errorf("[%d] method List returned list %#v that contains items other than expected - %#v",
				testN, l, test.want)
		}
	}
}

func TestSetIncludes(t *testing.T) {
	for testN, test := range append(setTestsAdd(), setTestsDel()...) {
		// New set
		s := NewSet(test.init...).Add(test.add...)

		// Check for all expected elements present
		for _, item := range test.want {
			if !s.Includes(item) {
				t.Errorf("[%d] set does not includes expected item - %v", testN, item)
			}
		}

		// Delete all that need to be deleted
		s.Del(test.del...)

		// Check for all deleted elements do not present
		for _, item := range test.del {
			if s.Includes(item) {
				t.Errorf("[%d] set still includes deleted item - %v", testN, item)
			}
		}
	}
}

func TestSetComplement(t *testing.T) {
	for testN, test := range setTestsComplement() {
		// New set
		s := NewSet(test.init...)

		// Get complement s by values from test.add
		complement := s.Complement(test.add...)

		// Make sorted expected result
		sorted := make([]string, len(test.want))
		copy(sorted, test.want)
		sort.Strings(sorted)

		// Compare complement with expected value from sorted
		if !reflect.DeepEqual(complement, sorted) {
			t.Errorf("[%d] got complement of set - %#v, want - %#v", testN, complement, sorted)
		}
	}
}

func TestSetAddComplement(t *testing.T) {
	for testN, test := range setTestsComplement() {
		// New set
		s := NewSet(test.init...)

		// Add items and get complement s by values from test.add
		complement := s.AddComplement(test.add...)

		// Make sorted expected result
		sorted := make([]string, len(test.want))
		copy(sorted, test.want)
		sort.Strings(sorted)

		// Compare complement with expected value from sorted
		if !reflect.DeepEqual(complement, sorted) {
			t.Errorf("[%d] got complement of set - %#v, want - %#v", testN, complement, sorted)
		}

		// Need to check that all items from test.init and test.add not in the set
		for _, item := range append(test.init, test.add...) {
			if !s.Includes(item) {
				t.Errorf("[%d] item %q not found in the resulted set", testN, item)
			}
		}

		// Need to remove all elements from both sets - resulted set should be empty
		if s = s.Del(append(test.init, test.add...)...); !s.Empty() {
			t.Errorf("[%d] set must be empty, but it contains items: %#v", testN, s)
		}
	}
}

func FuzzSetString(f *testing.F) {
	// Size on which set has to be cleaned up
	const cleanupSize = 10240

	set := NewSet[string]()
	f.Add("string set")

	f.Fuzz(func(t *testing.T, key string) {
		// Add key to the set
		set.Add(key)

		// Check for key present
		if !set[key] {
			t.Errorf("key %q was added but not found in the set", key)
		}

		// Check for cleanup size is not reached
		if set.Len() != cleanupSize {
			// Go to the next iteration
			return
		}

		// Cleanup set
		for _, item := range set.List() {
			set.Del(item)
		}

		// Check for non-empty set
		if sl := set.Len(); sl != 0 {
			t.Errorf("set is not empty after cleanup, len: %v", sl)
		}
	})
}

func FuzzSetInts(f *testing.F) {
	// Size on which set has to be cleaned up
	const cleanupSize = 1024

	set := NewSet[int]()
	f.Add(int(2022))

	f.Fuzz(func(t *testing.T, key int) {
		// Add key to the set
		set.Add(key)

		// Check for key present
		if !set[key] {
			t.Errorf("key %v was added but not found in the set", key)
		}

		// Check for cleanup size is not reached
		if set.Len() != cleanupSize {
			// Go to the next iteration
			return
		}

		// Cleanup set
		for _, item := range set.List() {
			set.Del(item)
		}

		// Check for non-empty set
		if sl := set.Len(); sl != 0 {
			t.Errorf("set is not empty after cleanup, len: %v", sl)
		}
	})
}

func newSet(test *stp) Set[string] {
	// Init new set
	return NewSet(test.init...).
		// Add items
		Add(test.add...).
		// Delete items
		Del(test.del...)
}
