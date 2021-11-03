package ovsdb

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"

	"k8s.io/klog/v2/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

var testSchemaGC = &libovsdb.DatabaseSchema{
	Name:    "gc",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"rootTable": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"name": {
					Type: libovsdb.TypeString,
				},
				"refSet": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type:     libovsdb.TypeUUID,
							RefTable: "table1",
						},
					},
				},
				"refMap": {
					Type: libovsdb.TypeMap,
					TypeObj: &libovsdb.ColumnType{
						Value: &libovsdb.BaseType{
							Type:     libovsdb.TypeUUID,
							RefTable: "table1",
						},
					},
				},
				"refUUID": {
					Type: libovsdb.TypeUUID,
					TypeObj: &libovsdb.ColumnType{
						Value: &libovsdb.BaseType{
							Type:     libovsdb.TypeUUID,
							RefTable: "table1",
						},
					},
				},
			},
			IsRoot: true,
		},
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"name": {
					Type: libovsdb.TypeString,
				},
				"refSet": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type:     libovsdb.TypeUUID,
							RefTable: "table2",
						},
					},
				},
			},
			IsRoot: false,
		},
		"table2": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"name": {
					Type: libovsdb.TypeString,
				},
			},
			IsRoot: false,
		},
	},
}

func TestCacheUpdateUUID(t *testing.T) {
	val := make(map[string]interface{})
	// table1
	t1UUID := uuid.NewString()
	t1ovsUUID := libovsdb.UUID{GoUUID: t1UUID}
	val[libovsdb.ColUuid] = t1ovsUUID
	val["name"] = "t1Row"
	key1 := common.NewDataKey("gc", "table1", t1UUID)
	buf1, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t1 := mvccpb.KeyValue{Key: []byte(key1.String()), Value: buf1}

	val = make(map[string]interface{})

	// root table
	rootUUID := uuid.NewString()
	rootOvsUUID := libovsdb.UUID{GoUUID: rootUUID}
	val[libovsdb.ColUuid] = rootOvsUUID
	val["name"] = "rootRow"
	val["refUUID"] = t1ovsUUID
	keyR := common.NewDataKey("gc", "rootTable", rootUUID)
	bufR, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_r := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}

	tCache := cache{}
	_, err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
	assert.Nil(t, err)
	dbCache := tCache.getDBCache("gc")

	// store values in the cache and check counters
	ePR := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	ePT2 := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_t1}
	dbCache.updateCache([]*clientv3.Event{&ePR, &ePT2})
	r, ok := dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// reset the cache and store values in a different order
	tCache = cache{}
	_, err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
	assert.Nil(t, err)
	dbCache = tCache.getDBCache("gc")
	dbCache.updateCache([]*clientv3.Event{&ePT2, &ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	//remove the root row
	eDR := clientv3.Event{Type: clientv3.EventTypeDelete, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&eDR})
	r, ok = dbCache.getRow(keyR)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)

	// return the root row
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// update root row
	val["refUUID"] = libovsdb.UUID{GoUUID: uuid.NewString()}
	bufR, err = json.Marshal(val)
	assert.Nil(t, err)
	kv_r = mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}
	ePR = clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
}

func TestCacheUpdateMap(t *testing.T) {
	val := make(map[string]interface{})
	// table1
	t1UUID := uuid.NewString()
	t1ovsUUID := libovsdb.UUID{GoUUID: t1UUID}
	val[libovsdb.ColUuid] = t1ovsUUID
	val["name"] = "t1Row"
	key1 := common.NewDataKey("gc", "table1", t1UUID)
	buf1, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t1 := mvccpb.KeyValue{Key: []byte(key1.String()), Value: buf1}

	val = make(map[string]interface{})

	// root table
	rootUUID := uuid.NewString()
	rootOvsUUID := libovsdb.UUID{GoUUID: rootUUID}
	val[libovsdb.ColUuid] = rootOvsUUID
	val["name"] = "rootRow"
	val["refMap"] = libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"t2": t1ovsUUID, "a": libovsdb.UUID{GoUUID: uuid.NewString()}}}
	keyR := common.NewDataKey("gc", "rootTable", rootUUID)
	bufR, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_r := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}

	tCache := cache{}
	_, err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
	assert.Nil(t, err)
	dbCache := tCache.getDBCache("gc")

	// store values in the cache and check counters
	ePR := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	ePT2 := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_t1}
	dbCache.updateCache([]*clientv3.Event{&ePR, &ePT2})
	r, ok := dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// reset the cache and store values in a different order
	tCache = cache{}
	_, err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
	assert.Nil(t, err)
	dbCache = tCache.getDBCache("gc")
	dbCache.updateCache([]*clientv3.Event{&ePT2, &ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	//remove the root row
	eDR := clientv3.Event{Type: clientv3.EventTypeDelete, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&eDR})
	r, ok = dbCache.getRow(keyR)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)

	// return the root row
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// update root row
	val["refMap"] = libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"t1": t1ovsUUID, "t2": libovsdb.UUID{GoUUID: uuid.NewString()}}}
	bufR, err = json.Marshal(val)
	assert.Nil(t, err)
	kv_r = mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}
	ePR = clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)
}

func TestCacheUpdateSet(t *testing.T) {
	val := make(map[string]interface{})

	// table1
	t1UUID := uuid.NewString()
	T1ovsUUID := libovsdb.UUID{GoUUID: t1UUID}
	val[libovsdb.ColUuid] = T1ovsUUID
	val["name"] = "t1Row"
	key1 := common.NewDataKey("gc", "table1", t1UUID)
	buf1, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t1 := mvccpb.KeyValue{Key: []byte(key1.String()), Value: buf1}

	val = make(map[string]interface{})

	// root table
	rootUUID := uuid.NewString()
	rootOvsUUID := libovsdb.UUID{GoUUID: rootUUID}
	val[libovsdb.ColUuid] = rootOvsUUID
	val["name"] = "rootRow"
	val["refSet"] = libovsdb.OvsSet{GoSet: []interface{}{T1ovsUUID, libovsdb.UUID{GoUUID: uuid.NewString()}}}
	keyR := common.NewDataKey("gc", "rootTable", rootUUID)
	bufR, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_r := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}

	tCache := cache{}
	_, err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
	assert.Nil(t, err)
	dbCache := tCache.getDBCache("gc")

	// store values in the cache and check counters
	ePR := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	ePT2 := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_t1}
	dbCache.updateCache([]*clientv3.Event{&ePR, &ePT2})
	r, ok := dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// reset the cache and store values in a different order
	tCache = cache{}
	_, err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
	assert.Nil(t, err)
	dbCache = tCache.getDBCache("gc")
	dbCache.updateCache([]*clientv3.Event{&ePT2, &ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	//remove the root row
	eDR := clientv3.Event{Type: clientv3.EventTypeDelete, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&eDR})
	r, ok = dbCache.getRow(keyR)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)

	// return the root row
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// update root row
	val["refSet"] = libovsdb.OvsSet{GoSet: []interface{}{libovsdb.UUID{GoUUID: uuid.NewString()}}}
	bufR, err = json.Marshal(val)
	assert.Nil(t, err)
	kv_r = mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}
	ePR = clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
}

func TestCacheCheckUUID(t *testing.T) {
	val1UUID := uuid.NewString()
	val1 := libovsdb.UUID{GoUUID: val1UUID}
	counts := checkCounters(nil, val1, libovsdb.TypeUUID)
	assert.Equal(t, counts[val1UUID], -1)
	counts = checkCounters(val1, nil, libovsdb.TypeUUID)
	assert.Equal(t, counts[val1UUID], 1)
	val2UUID := uuid.NewString()
	val2 := libovsdb.UUID{GoUUID: val2UUID}
	counts = checkCounters(val1, val2, libovsdb.TypeUUID)
	assert.Equal(t, counts[val1UUID], 1)
	assert.Equal(t, counts[val2UUID], -1)
}

func TestCacheCheckSet(t *testing.T) {
	val1UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	val2UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	val3UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	val4UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	set1 := libovsdb.OvsSet{GoSet: []interface{}{val1UUID, val2UUID}}
	counts := checkCounters(nil, set1, libovsdb.TypeSet)
	assert.Equal(t, -1, counts[val1UUID.GoUUID])
	assert.Equal(t, -1, counts[val2UUID.GoUUID])
	counts = checkCounters(set1, nil, libovsdb.TypeSet)
	assert.Equal(t, counts[val1UUID.GoUUID], 1)
	assert.Equal(t, counts[val2UUID.GoUUID], 1)
	set2 := libovsdb.OvsSet{GoSet: []interface{}{val3UUID, val4UUID}}
	counts = checkCounters(set1, set2, libovsdb.TypeSet)
	assert.Equal(t, counts[val1UUID.GoUUID], 1)
	assert.Equal(t, counts[val2UUID.GoUUID], 1)
	assert.Equal(t, counts[val3UUID.GoUUID], -1)
	assert.Equal(t, counts[val4UUID.GoUUID], -1)
	counts = checkCounters(val1UUID, val2UUID, libovsdb.TypeSet)
	assert.Equal(t, counts[val1UUID.GoUUID], 1)
	assert.Equal(t, counts[val2UUID.GoUUID], -1)
}

func TestCacheCheckMap(t *testing.T) {
	val1UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	val2UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	val3UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	val4UUID := libovsdb.UUID{GoUUID: uuid.NewString()}
	map1 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"a": val1UUID, "b": val2UUID}}
	counts := checkCounters(nil, map1, libovsdb.TypeMap)
	assert.Equal(t, -1, counts[val1UUID.GoUUID])
	assert.Equal(t, -1, counts[val2UUID.GoUUID])
	counts = checkCounters(map1, nil, libovsdb.TypeMap)
	assert.Equal(t, 1, counts[val1UUID.GoUUID])
	assert.Equal(t, 1, counts[val2UUID.GoUUID])
	map2 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"a": val3UUID, "b": val4UUID}}
	counts = checkCounters(map1, map2, libovsdb.TypeMap)
	assert.Equal(t, 1, counts[val1UUID.GoUUID])
	assert.Equal(t, 1, counts[val2UUID.GoUUID])
	assert.Equal(t, -1, counts[val3UUID.GoUUID])
	assert.Equal(t, -1, counts[val4UUID.GoUUID])
	map11 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"c": val1UUID, "d": val2UUID}}
	counts = checkCounters(map1, map11, libovsdb.TypeMap)
	assert.Equal(t, 0, counts[val1UUID.GoUUID])
	assert.Equal(t, 0, counts[val2UUID.GoUUID])
}
