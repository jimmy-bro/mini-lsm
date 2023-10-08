package utils

import (
	"runtime/debug"

	"github.com/sirupsen/logrus"
)

func Assert(assert bool, message string) {
	if !assert {
		debug.PrintStack()
		logrus.Fatalln(message)
	}
}

func Assertf(assert bool, message string, params ...interface{}) {
	if !assert {
		debug.PrintStack()
		logrus.Fatalf(message, params...)
	}
}
