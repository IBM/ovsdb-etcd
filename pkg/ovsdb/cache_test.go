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
				"_uuid" : {
					Type: libovsdb.TypeUUID,
				},
				"refSet": {
					Type:    libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
							RefTable: "table1",
						},
					},
				},
				"refMap": {
					Type:    libovsdb.TypeMap,
					TypeObj: &libovsdb.ColumnType{
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
							RefTable: "table2",
						},
					},
				},
				"refUUID": {
					Type:    libovsdb.TypeUUID,
					TypeObj: &libovsdb.ColumnType{
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
							RefTable: "table2",
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
				"_uuid" : {
					Type: libovsdb.TypeUUID,
				},
				"refSet": {
					Type:    libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
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
				"_uuid" : {
					Type: libovsdb.TypeUUID,
				},
			},

		},
	},
}

func TestCacheUpdateUUID(t *testing.T) {
	val := make(map[string]interface{})
	// table2
	t2UUID := uuid.NewString()
	t2ovsUUID := libovsdb.UUID{GoUUID: t2UUID}
	val[libovsdb.COL_UUID] = t2ovsUUID
	val["name"] = "t2Row"
	key2 := common.NewDataKey("gc", "table2", t2UUID)
	buf2, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t2 := mvccpb.KeyValue{Key: []byte(key2.String()), Value: buf2}

	val = make(map[string]interface{})

	// root table
	rootUUID := uuid.NewString()
	rootOvsUUID := libovsdb.UUID{GoUUID: rootUUID}
	val[libovsdb.COL_UUID] = rootOvsUUID
	val["name"] = "rootRow"
	val["refUUID"] = t2ovsUUID
	keyR := common.NewDataKey("gc", "rootTable", rootUUID)
	bufR, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_r := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}

	tCache := cache{}
	err = tCache.addDatabaseCache(testSchemaGC, nil,  klogr.New())
	assert.Nil(t, err)
	dbCache := tCache.getDBCache("gc")

	// store values in the cache and check counters
	ePR := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	ePT2 := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_t2}
	dbCache.updateCache([]*clientv3.Event{ &ePR, &ePT2})
	r, ok := dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// reset the cache and store values in a different order
	tCache = cache{}
	err = tCache.addDatabaseCache(testSchemaGC, nil,  klogr.New())
	assert.Nil(t, err)
	dbCache = tCache.getDBCache("gc")
	dbCache.updateCache([]*clientv3.Event{ &ePT2, &ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	//remove the root row
	eDR := clientv3.Event{Type: clientv3.EventTypeDelete, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&eDR})
	r, ok = dbCache.getRow(keyR)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)

	// return the root row
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key2)
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
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
}

func TestCacheUpdateMap(t *testing.T) {
	val := make(map[string]interface{})
	// table2
	t2UUID := uuid.NewString()
	t2ovsUUID := libovsdb.UUID{GoUUID: t2UUID}
	val[libovsdb.COL_UUID] = t2ovsUUID
	val["name"] = "t2Row"
	key2 := common.NewDataKey("gc", "table2", t2UUID)
	buf2, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t2 := mvccpb.KeyValue{Key: []byte(key2.String()), Value: buf2}

	val = make(map[string]interface{})

	// root table
	rootUUID := uuid.NewString()
	rootOvsUUID := libovsdb.UUID{GoUUID: rootUUID}
	val[libovsdb.COL_UUID] = rootOvsUUID
	val["name"] = "rootRow"
	val["refMap"] = libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"t2" : t2ovsUUID, "a" : libovsdb.UUID{GoUUID: uuid.NewString()}}}
	keyR := common.NewDataKey("gc", "rootTable", rootUUID)
	bufR, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_r := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}

	tCache := cache{}
	err = tCache.addDatabaseCache(testSchemaGC, nil,  klogr.New())
	assert.Nil(t, err)
	dbCache := tCache.getDBCache("gc")

	// store values in the cache and check counters
	ePR := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	ePT2 := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_t2}
	dbCache.updateCache([]*clientv3.Event{ &ePR, &ePT2})
	r, ok := dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// reset the cache and store values in a different order
	tCache = cache{}
	err = tCache.addDatabaseCache(testSchemaGC, nil,  klogr.New())
	assert.Nil(t, err)
	dbCache = tCache.getDBCache("gc")
	dbCache.updateCache([]*clientv3.Event{ &ePT2, &ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	//remove the root row
	eDR := clientv3.Event{Type: clientv3.EventTypeDelete, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&eDR})
	r, ok = dbCache.getRow(keyR)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)

	// return the root row
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// update root row
	val["refMap"] = libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"t1" : t2ovsUUID, "t2" : libovsdb.UUID{GoUUID: uuid.NewString()}}}
	bufR, err = json.Marshal(val)
	assert.Nil(t, err)
	kv_r = mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}
	ePR = clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&ePR})
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)
}

func TestCacheUpdateSet(t *testing.T) {
	val := make(map[string]interface{})

	// table1
	t1UUID := uuid.NewString()
	T1ovsUUID := libovsdb.UUID{GoUUID: t1UUID}
	val[libovsdb.COL_UUID] = T1ovsUUID
	val["name"] = "t1Row"
	key1 := common.NewDataKey("gc", "table1", t1UUID)
	buf1, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t1 := mvccpb.KeyValue{Key: []byte(key1.String()), Value: buf1}

	val = make(map[string]interface{})

	// root table
	rootUUID := uuid.NewString()
	rootOvsUUID := libovsdb.UUID{GoUUID: rootUUID}
	val[libovsdb.COL_UUID] = rootOvsUUID
	val["name"] = "rootRow"
	val["refSet"] = libovsdb.OvsSet{GoSet: []interface{}{T1ovsUUID, libovsdb.UUID{GoUUID: uuid.NewString()}}}
	keyR := common.NewDataKey("gc", "rootTable", rootUUID)
	bufR, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_r := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}

	tCache := cache{}
	err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
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
	err = tCache.addDatabaseCache(testSchemaGC, nil, klogr.New())
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

// Create and store objects:
// root -> t1 and t2
// root -> t2
// t1 -> t2
func TestCache(t *testing.T) {
	val := make(map[string]interface{})

	// table2
	t2UUID := uuid.NewString()
	t2ovsUUID := libovsdb.UUID{GoUUID: t2UUID}
	val[libovsdb.COL_UUID] = t2ovsUUID
	val["name"] = "t2Row"
	key2 := common.NewDataKey("gc", "table2", t2UUID)
	buf2, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t2 := mvccpb.KeyValue{Key: []byte(key2.String()), Value: buf2}

	val = make(map[string]interface{})

	// table1
	t1UUID := uuid.NewString()
	T1ovsUUID := libovsdb.UUID{GoUUID: t1UUID}
	val[libovsdb.COL_UUID] = T1ovsUUID
	val["name"] = "t1Row"
	val["refSet"] = libovsdb.OvsSet{GoSet: []interface{}{t2ovsUUID}}
	key1 := common.NewDataKey("gc", "table1", t1UUID)
	buf1, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_t1 := mvccpb.KeyValue{Key: []byte(key1.String()), Value: buf1}

	val = make(map[string]interface{})

	// root table
	rootUUID := uuid.NewString()
	rootOvsUUID := libovsdb.UUID{GoUUID: rootUUID}
	val[libovsdb.COL_UUID] = rootOvsUUID
	val["name"] = "rootRow"
	val["refSet"] = libovsdb.OvsSet{GoSet: []interface{}{T1ovsUUID, libovsdb.UUID{GoUUID: uuid.NewString()}}}
	val["refMap"] = libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"t2" : t2ovsUUID, "a" : libovsdb.UUID{GoUUID: uuid.NewString()}}}
	keyR := common.NewDataKey("gc", "rootTable", rootUUID)
	bufR, err := json.Marshal(val)
	assert.Nil(t, err)
	kv_r := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}

	tCache := cache{}
	err = tCache.addDatabaseCache(testSchemaGC, nil,  klogr.New())
	assert.Nil(t, err)
	dbCache := tCache.getDBCache("gc")

	// store values in the cache and check counters
	err = dbCache.storeValues([]*mvccpb.KeyValue{ &kv_t1, &kv_r, &kv_t2 })
	assert.Nil(t, err)
	r, ok := dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 2, r.counter)

	// reset the cache
	tCache = cache{}
	err = tCache.addDatabaseCache(testSchemaGC, nil,  klogr.New())
	assert.Nil(t, err)
	dbCache = tCache.getDBCache("gc")

	// store values in the cache in a different order
	err = dbCache.storeValues([]*mvccpb.KeyValue{ &kv_r, &kv_t1, &kv_t2})
	assert.Nil(t, err)
	r, ok = dbCache.getRow(keyR)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 2, r.counter)

	// delete a row from table 1, should reduce counter of the row in table2
	eD1 := clientv3.Event{Type: clientv3.EventTypeDelete, Kv: &kv_t1}
	dbCache.updateCache([]*clientv3.Event{&eD1})
	r, ok = dbCache.getRow(key1)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 1, r.counter)

	// return the row in table1, counter of the row in table2 should increase
	eP1 := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_t1}
	dbCache.updateCache([]*clientv3.Event{&eP1})
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 2, r.counter)

	val["refSet"] = libovsdb.OvsSet{GoSet: []interface{}{libovsdb.UUID{GoUUID: uuid.NewString()}}}
	val["refMap"] = libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"t3" : t2ovsUUID, "b" : libovsdb.UUID{GoUUID: uuid.NewString()}}}
	bufR, err = json.Marshal(val)
	assert.Nil(t, err)
	kv_rn := mvccpb.KeyValue{Key: []byte(keyR.String()), Value: bufR}
	eUR := clientv3.Event{Type: clientv3.EventTypePut, Kv: &kv_rn}
	dbCache.updateCache([]*clientv3.Event{&eUR})
	r, ok = dbCache.getRow(key1)
	assert.True(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 2, r.counter)


	// remove rows from table1 and root, counter of the row in table2 should be 0
	eDR := clientv3.Event{Type: clientv3.EventTypeDelete, Kv: &kv_r}
	dbCache.updateCache([]*clientv3.Event{&eDR, &eD1})
	r, ok = dbCache.getRow(keyR)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key1)
	assert.False(t, ok)
	r, ok = dbCache.getRow(key2)
	assert.True(t, ok)
	assert.Equal(t, 0, r.counter)
}