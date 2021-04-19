package libovsdb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// empty Set test
func TestEmptySet(t *testing.T) {
	emptySet, err := NewOvsSet([]string{})
	assert.Nil(t, err)
	jsonStr, err := json.Marshal(emptySet)
	assert.Nil(t, err)
	expected := "[\"set\",[]]"
	assert.JSONEqf(t, expected, string(jsonStr), "they should be equal\n")
}

// test Set
func TestSet(t *testing.T) {
	ovsSet, err := NewOvsSet([]string{"aa", "bb"})
	assert.Nil(t, err)
	jsonStr, err := json.Marshal(ovsSet)
	assert.Nil(t, err)
	expected := "[\"set\",[\"aa\",\"bb\"]]"
	require.JSONEqf(t, expected, string(jsonStr), "they should be equal\n")
}

// empty Map test
func TestEmptyMap(t *testing.T) {
	emptyMap, err := NewOvsMap(map[string]string{})
	assert.Nil(t, err)
	jsonStr, err := json.Marshal(emptyMap)
	assert.Nil(t, err)
	expected := "[\"map\",[]]"
	assert.JSONEqf(t, expected, string(jsonStr), "they should be equal\n")
}

// test Map
func TestMap(t *testing.T) {
	ovsMap, err := NewOvsMap(map[string]string{"one": "first", "two": "second"})
	assert.Nil(t, err)
	jsonStr, err := json.Marshal(ovsMap)
	assert.Nil(t, err)
	expected := "[\"map\",[[\"one\",\"first\"],[\"two\",\"second\"]]]"
	assert.JSONEqf(t, expected, string(jsonStr), "they should be equal\n")
}
