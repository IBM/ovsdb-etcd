package common

import (
	"io/ioutil"
	"os"
)

func ReadFile(filename string) (data []byte, err error) {
	var jsonFile *os.File
	jsonFile, err = os.Open(filename)
	if err != nil {
		return
	}
	defer func() {
		e := jsonFile.Close()
		if e != nil {
			if err == nil {
				err = e
			}
		}
	}()
	return ioutil.ReadAll(jsonFile)
}
