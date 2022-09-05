package dbi

import "github.com/r-che/log"

// Log ID
var logID string

func setLogID(id, suffix string) {
	if suffix == "" {
		logID = "(" + id + ") "
	} else {
		logID = "(" + id + ":" + suffix + ") "
	}
}

func logDbg(format string, v ...any) {
	log.D(logID + format, v...)
}

func logInf(format string, v ...any) {
	log.I(logID + format, v...)
}

func logWrn(format string, v ...any) {
	log.W(logID + format, v...)
}

func logErr(format string, v ...any) {
	log.E(logID + format, v...)
}

func logFat(format string, v ...any) {
	log.F(logID + format, v...)
}
