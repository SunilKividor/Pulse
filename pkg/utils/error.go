package utils

import (
	"fmt"
	"log"
)

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s,%s", msg, err.Error())
	}
}

func FailOnErrorWithoutPanic(err error, msg string) error {
	if err != nil {
		log.Printf("%s,%s", msg, err.Error())
	}

	return fmt.Errorf("%s,%s", msg, err.Error())
}
