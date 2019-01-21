package main

import (
	"os"
	"log"
)

var (
	dbgLevel int
	dbgLogger *log.Logger
)

func dbgSet(lvl int) {
	dbgLevel = lvl
}

func dbg(lvl int, where string, fmt string, v ...interface{}) {
	if lvl <= dbgLevel {
		dbgLogger.Printf(where + ": " + fmt + "\n", v...)
	}
}

func dbgErr(lvl int, where string, err error) {
	if lvl <= dbgLevel {
		dbgLogger.Printf("%s: error: %s\n", where, err.Error())
	}
}

func die(where string, fmt string, v ...interface{}) {
	dbgLogger.Fatalf(where + ": " + fmt + "\n", v...)
}

func dieErr(where string, err error) {
	dbgLogger.Fatalf("%s: fatal error: %s\n", where, err.Error())
}

func init() {
	dbgLogger = log.New(os.Stderr, "", log.LstdFlags | log.LUTC)
}
