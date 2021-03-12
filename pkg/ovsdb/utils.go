package ovsdb

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/ebay/libovsdb"
	"github.com/stretchr/testify/assert"
)

func readJson(t *testing.T, filename string) []byte {
	jsonFile, err := os.Open(filename)
	assert.Nil(t, err)
	defer jsonFile.Close()
	byteValue, err := ioutil.ReadAll(jsonFile)
	assert.Nil(t, err)
	return byteValue
}

func bytesToInterface(byteValue []byte) *interface{} {
	var result interface{}
	json.Unmarshal([]byte(byteValue), &result)
	return &result
}

func bytesToMapInterface(byteValue []byte) *map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(byteValue), &result)
	return &result
}

func bytesToArrayMapInterface(byteValue []byte) *map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(byteValue), &result)
	return &result
}

func bytesToMapString(byteValue []byte) *map[string]string {
	var result map[string]string
	json.Unmarshal([]byte(byteValue), &result)
	return &result
}

func bytesToArrayMapString(byteValue []byte) *[]map[string]string {
	var result []map[string]string
	json.Unmarshal([]byte(byteValue), &result)
	return &result
}

func bytesToArrayInterface(byteValue []byte) *[]interface{} {
	var result []interface{}
	json.Unmarshal([]byte(byteValue), &result)
	return &result
}

func bytesToOperation(byteValue []byte) *libovsdb.Operation {
	var result libovsdb.Operation
	json.Unmarshal([]byte(byteValue), &result)
	return &result
}

func arrayMapStringToArrayInterface(in []map[string]string) (out []interface{}) {
	for _, v := range in {
		out = append(out, v)
	}
	return
}
