package common

import (
	"encoding/json"

	"github.com/ebay/libovsdb"
)

func BytesToInterface(in []byte) *interface{} {
	var result interface{}
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToMapInterface(in []byte) *map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToArrayMapInterface(in []byte) *map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToMapString(in []byte) *map[string]string {
	var result map[string]string
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToArrayMapString(in []byte) *[]map[string]string {
	var result []map[string]string
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToArrayInterface(in []byte) *[]interface{} {
	var result []interface{}
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToOperation(in []byte) *libovsdb.Operation {
	var result libovsdb.Operation
	json.Unmarshal([]byte(in), &result)
	return &result
}

func ArrayToMap(in []interface{}) map[interface{}]bool {
	ret := map[interface{}]bool{}
	for _, str := range in {
		ret[str] = true
	}
	return ret
}

func ArrayMapStringToArrayInterface(in []map[string]string) (out []interface{}) {
	for _, v := range in {
		out = append(out, v)
	}
	return
}
