package libovsdb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type marshalSetTestTuple struct {
	objInput           interface{}
	jsonExpectedOutput string
}

type marshalMapsTestTuple struct {
	objInput           map[string]string
	jsonExpectedOutput string
}

var validUUIDStr0 = `00000000-0000-0000-0000-000000000000`
var validUUIDStr1 = `11111111-1111-1111-1111-111111111111`
var validUUID0 = UUID{GoUUID: validUUIDStr0}
var validUUID1 = UUID{GoUUID: validUUIDStr1}

var setTestList = []marshalSetTestTuple{
	{
		objInput:           []string{},
		jsonExpectedOutput: `["set",[]]`,
	},
	{
		objInput:           `aa`,
		jsonExpectedOutput: `"aa"`,
	},
	{
		objInput:           false,
		jsonExpectedOutput: `false`,
	},
	{
		objInput:           float64(10),
		jsonExpectedOutput: `10`,
	},
	{
		objInput:           10.2,
		jsonExpectedOutput: `10.2`,
	},
	{
		objInput:           []string{`aa`},
		jsonExpectedOutput: `"aa"`,
	},
	{
		objInput:           [1]string{`aa`},
		jsonExpectedOutput: `"aa"`,
	},
	{
		objInput:           []string{`aa`, `bb`},
		jsonExpectedOutput: `["set",["aa","bb"]]`,
	},
	{
		objInput:           [2]string{`aa`, `bb`},
		jsonExpectedOutput: `["set",["aa","bb"]]`,
	},
	{
		objInput:           []float64{10.2, 15.4},
		jsonExpectedOutput: `["set",[10.2,15.4]]`,
	},
	{
		objInput:           []UUID{},
		jsonExpectedOutput: `["set",[]]`,
	},
	{
		objInput:           UUID{GoUUID: `aa`},
		jsonExpectedOutput: `["named-uuid","aa"]`,
	},
	{
		objInput:           []UUID{{GoUUID: `aa`}},
		jsonExpectedOutput: `["named-uuid","aa"]`,
	},
	{
		objInput:           []UUID{{GoUUID: `aa`}, {GoUUID: `bb`}},
		jsonExpectedOutput: `["set",[["named-uuid","aa"],["named-uuid","bb"]]]`,
	},
	{
		objInput:           validUUID0,
		jsonExpectedOutput: fmt.Sprintf(`["uuid","%v"]`, validUUIDStr0),
	},
	{
		objInput:           []UUID{validUUID0},
		jsonExpectedOutput: fmt.Sprintf(`["uuid","%v"]`, validUUIDStr0),
	},
	{
		objInput:           []UUID{validUUID0, validUUID1},
		jsonExpectedOutput: fmt.Sprintf(`["set",[["uuid","%v"],["uuid","%v"]]]`, validUUIDStr0, validUUIDStr1),
	},
}

var mapTestList = []marshalMapsTestTuple{
	{
		objInput:           map[string]string{},
		jsonExpectedOutput: `["map",[]]`,
	},
	{
		objInput:           map[string]string{`v0`: `k0`},
		jsonExpectedOutput: `["map",[["v0","k0"]]]`,
	},
	{
		objInput:           map[string]string{`v0`: `k0`, `v1`: `k1`},
		jsonExpectedOutput: `["map",[["v0","k0"],["v1","k1"]]]`,
	},
}

func TestMap(t *testing.T) {
	for _, e := range mapTestList {
		m, err := NewOvsMap(e.objInput)
		assert.Nil(t, err)
		jsonStr, err := json.Marshal(m)
		assert.Nil(t, err)
		compareJSONMapSets(t, string(jsonStr), e.jsonExpectedOutput)

		var res OvsMap
		err = json.Unmarshal(jsonStr, &res)
		assert.Nil(t, err)
		assert.Equal(t, *m, res, "they should be equal\n")
	}
}

func TestSet(t *testing.T) {
	for _, e := range setTestList {
		set, err := NewOvsSet(e.objInput)
		assert.Nil(t, err)
		jsonStr, err := json.Marshal(set)
		assert.Nil(t, err)
		compareJSONMapSets(t, string(jsonStr), e.jsonExpectedOutput)

		var res OvsSet
		err = json.Unmarshal(jsonStr, &res)
		assert.Nil(t, err)
		assert.ElementsMatch(t, set.GoSet, res.GoSet, "they should have the same elements\n")
	}
}

func compareJSONMapSets(t *testing.T, jsonStr string, expected string) {
	var expectedInterface interface{}
	var jsonInterface interface{}
	err := json.Unmarshal([]byte(expected), &expectedInterface)
	assert.Nil(t, err)
	err = json.Unmarshal([]byte(jsonStr), &jsonInterface)
	assert.Nil(t, err)
	jsonSlice, ok1 := jsonInterface.([]interface{})
	expectedSlice, ok2 := expectedInterface.([]interface{})
	if ok1 && ok2 {
		assert.Equal(t, expectedSlice[0], jsonSlice[0], "they should both start with 'map', 'set' or 'uuid")
		if _, uuid := expectedSlice[1].(string); uuid {
			// single uuid or named-uuid element
			assert.Equal(t, expectedSlice[1], jsonSlice[1], "they should be equals")
		} else {
			assert.ElementsMatch(t, expectedSlice[1].([]interface{}), jsonSlice[1].([]interface{}), "they should have the same elements\n")
		}
	} else {
		// single set elements
		assert.Equal(t, expectedInterface, jsonInterface, "they should be equals")
	}
}
