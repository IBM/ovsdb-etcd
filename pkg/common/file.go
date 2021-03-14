package common

import (
	"io/ioutil"
	"os"
)

func ReadFile(filename string) ([]byte, error) {
	byteValue := []byte{}
	jsonFile, err := os.Open(filename)
	if err != nil {
		return byteValue, err
	}
	defer jsonFile.Close()
	return ioutil.ReadAll(jsonFile)
}
