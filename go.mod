module github.com/r-che/dfi

go 1.18

require (
	github.com/fsnotify/fsnotify v1.5.4
	github.com/r-che/log v0.0.0-00010101000000-000000000000
	github.com/r-che/optsparser v0.0.0-00010101000000-000000000000
)

require golang.org/x/sys v0.0.0-20220412211240-33da011f77ad // indirect

replace github.com/r-che/optsparser => ../optsparser

replace github.com/r-che/log => ../log
