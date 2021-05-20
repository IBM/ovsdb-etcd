package common

import (
	"io/ioutil"
	"os"
)

func ReadFile(filename string) ([]byte, error) {
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()
	return ioutil.ReadAll(jsonFile)
}
