package ovsdb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

var testSchemaSimple *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
	Name:    "simple",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"key1": {
					Type: libovsdb.TypeString,
				},
				"key2": {
					Type: libovsdb.TypeInteger,
				},
			},
		},
	},
}

var testSchemaAtomic *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
	Name:    "atomic",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"string": {
					Type: libovsdb.TypeString,
				},
				"integer": {
					Type: libovsdb.TypeInteger,
				},
				"real": {
					Type: libovsdb.TypeReal,
				},
				"boolean": {
					Type: libovsdb.TypeBoolean,
				},
				"uuid": {
					Type: libovsdb.TypeUUID,
				},
			},
		},
	},
}

var testSchemaMutable *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
	Name:    "mutable",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"mutable": {
					Type: libovsdb.TypeInteger,
				},
				"unmutable": {
					Type:    libovsdb.TypeInteger,
					Mutable: new(bool),
				},
			},
		},
	},
}

var testSchemaEnum *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
	Name:    "enum",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"color": {
					Type: libovsdb.TypeEnum,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
							Enum: &libovsdb.OvsSet{GoSet: []interface{}{"red", "green", "blue"}},
						},
					},
				},
				"animal": {
					Type: libovsdb.TypeEnum,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
							Enum: &libovsdb.OvsSet{GoSet: []interface{}{"dog", "cat", "mouse"}},
						},
					},
				},
			},
		},
	},
}

var testSchemaSet *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
	Name:    "set",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"string": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Max: libovsdb.Unlimited,
						Min: 0,
					},
				},
				"integer": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Max: 2,
						Min: 0,
					},
				},
				"uuid": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
						},
						Max: libovsdb.Unlimited,
						Min: 0,
					},
				},
			},
		},
	},
}

var testSchemaMap *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
	Name:    "map",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"string": {
					Type: libovsdb.TypeMap,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Min: 0,
						Max: libovsdb.Unlimited,
					},
				},
			},
		},
	},
}

var testSchemaUUID *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
	Name:    "uuid",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"uuid": {
					Type: libovsdb.TypeUUID,
				},
			},
		},
		"table2": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"map": {
					Type: libovsdb.TypeMap,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
						},
						Min: 1,
						Max: libovsdb.Unlimited,
					},
				},
			},
		},
	},
}

func testEtcdNewCli() (*clientv3.Client, error) {
	endpoints := []string{"http://127.0.0.1:2379"}
	return NewEtcdClient(endpoints)
}

func testEtcdCleanup(t *testing.T) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	_, err = cli.Delete(ctx, "", clientv3.WithPrefix())
	assert.Nil(t, err)
}

func testMergeKvs(kvs []*mvccpb.KeyValue, table string) (*map[string]interface{}, error) {
	dump := &map[string]interface{}{}
	for _, x := range kvs {
		kv, err := NewKeyValue(x)
		if err != nil {
			return nil, err
		}
		if kv.Key.TableName != table {
			continue
		}
		for k, v := range kv.Value {
			(*dump)[k] = v
		}
	}
	return dump, nil
}

func testEtcdDump(t *testing.T, dbname, table string) map[string]interface{} {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	key := common.NewTableKey(dbname, table)
	res, err := cli.Get(ctx, key.TableKeyString(), clientv3.WithPrefix())
	dump, err := testMergeKvs(res.Kvs, table)
	assert.Nil(t, err)
	return *dump
}

func testEtcdPut(t *testing.T, dbname, table string, row map[string]interface{}) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	key := common.GenerateDataKey(dbname, table)
	setRowUUID(&row, key.UUID)
	val, err := makeValue(&row)
	assert.Nil(t, err)
	_, err = cli.Put(ctx, key.String(), val)
	assert.Nil(t, err)
}

func testTransact(t *testing.T, req *libovsdb.Transact) (*libovsdb.TransactResponse, *Transaction) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer cli.Close()
	txn := NewTransaction(cli, "127.0.0.1:6642", "11", req)
	txn.AddSchema(testSchemaSimple)
	txn.AddSchema(testSchemaAtomic)
	txn.AddSchema(testSchemaMutable)
	txn.AddSchema(testSchemaEnum)
	txn.AddSchema(testSchemaSet)
	txn.AddSchema(testSchemaMap)
	txn.AddSchema(testSchemaUUID)
	txn.Commit()
	return &txn.response, txn
}

func testTransactDump(t *testing.T, txn *Transaction, dbname, table string) map[string]interface{} {
	dump := map[string]interface{}{}
	databaseCache, ok := txn.cache[dbname]
	assert.True(t, ok)
	tableCache, ok := databaseCache[table]
	assert.True(t, ok)
	for _, row := range tableCache {
		for k, v := range *row {
			if k == COL_UUID || k == COL_VERSION {
				continue
			}
			dump[k] = v
		}
	}
	return dump
}

func TestTransactInsertSimple(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"key1": "val1",
	}
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, int(0), dump["key2"])
}

func testTransactInsertSimpleScale(t *testing.T, n int) {
	table := "table1"
	row := map[string]interface{}{
		"key1": "val1",
	}
	op := libovsdb.Operation{
		Op:    OP_INSERT,
		Table: &table,
		Row:   &row,
	}

	req := &libovsdb.Transact{
		DBName:     "simple",
		Operations: []libovsdb.Operation{},
	}
	for i := 0; i < n; i++ {
		req.Operations = append(req.Operations, op)
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, int(0), dump["key2"])
}

func TestTransactInsertSimpleScale10(t *testing.T) {
	testTransactInsertSimpleScale(t, 10)
}

func TestTransactInsertSimpleScale100(t *testing.T) {
	testTransactInsertSimpleScale(t, 100)
}

/*
XXX: If run with default setting then it will fail with message:
XXX:
XXX:   etcdserver: too many operations in txn request
XXX:
XXX: To enable the test to pass:
XXX: 1. use etcd version 3.4.x and above
XXX: 2. configure in /etc/default/etcd:
XXX:
XXX:   ETCD_MAX_TXN_OPS=100000
XXX:   ETCD_MAX_REQUEST_BYTES=100000000
XXX:
func TestTransactInsertSimpleScale1000(t *testing.T) {
	testTransactInsertSimpleScale(t, 1000)
}
*/

func TestTransactInsertSimpleWithUUID(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"key1": "val1",
	}
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row,
				UUID:  &libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, int(0), dump["key2"])
}

func TestTransactInsertSimpleWithUUIDName(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuidName := "myuuid"
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:       OP_INSERT,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, int(0), dump["key2"])
}

func TestTransactInsertSimpleWithUUIDNameDupError(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuidName := "myuuid"
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:       OP_INSERT,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
			{
				Op:       OP_INSERT,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactAtomicInsertNamedUUID(t *testing.T) {
	table := "table1"
	uuidName1 := libovsdb.UUID{GoUUID: "myuuid1"}
	uuid1 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	uuid1buf, err := json.Marshal(uuid1)
	assert.Nil(t, err)
	uuid1array := []interface{}{}
	err = json.Unmarshal(uuid1buf, &uuid1array)
	assert.Nil(t, err)

	row1 := map[string]interface{}{
		"string": "val1",
	}
	row2 := map[string]interface{}{
		"uuid": &uuidName1,
	}

	req1 := &libovsdb.Transact{
		DBName: "atomic",
		Operations: []libovsdb.Operation{
			{
				Op:       OP_INSERT,
				Table:    &table,
				Row:      &row1,
				UUID:     &uuid1,
				UUIDName: &uuidName1.GoUUID,
			},
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
			},
		},
	}

	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)

	resp, _ := testTransact(t, req1)
	assert.Nil(t, resp.Error)

	dump := testEtcdDump(t, "atomic", "table1")
	assert.Equal(t, []interface{}{"uuid", uuid1.GoUUID}, dump["uuid"])
}

func TestTransactAtomicInsertUUID(t *testing.T) {
	table := "table1"
	uuid1 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	uuid1buf, err := json.Marshal(uuid1)
	assert.Nil(t, err)
	uuid1array := []interface{}{}
	err = json.Unmarshal(uuid1buf, &uuid1array)
	assert.Nil(t, err)

	row1 := map[string]interface{}{
		"string": "val1",
	}

	row2 := map[string]interface{}{
		"string": "val2",
	}

	where2 := []interface{}{
		[]interface{}{"_uuid", "==", uuid1array},
	}

	req1 := &libovsdb.Transact{
		DBName: "atomic",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
				UUID:  &uuid1,
			},
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
				Where: &where2,
			},
		},
	}

	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)

	resp, _ := testTransact(t, req1)
	assert.Nil(t, resp.Error)

	dump := testEtcdDump(t, "atomic", "table1")
	assert.Equal(t, []interface{}{"uuid", uuid1.GoUUID}, dump["_uuid"])
}

func TestTransactInsertEnumOk(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"color": "red",
	}
	req := &libovsdb.Transact{
		DBName: "enum",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "enum", "table1")
	assert.Equal(t, "red", dump["color"])
}

func TestTransactInsertEnumError(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"animal": "red",
	}
	req := &libovsdb.Transact{
		DBName: "enum",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactInsertSetOk(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"string": libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}},
	}
	req := &libovsdb.Transact{
		DBName: "set",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "set", "table1")
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}}, dump["string"])
}

func TestTransactInsertSetError(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"string": libovsdb.OvsSet{GoSet: []interface{}{1, 2, 3}},
	}
	req := &libovsdb.Transact{
		DBName: "set",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactSelect(t *testing.T) {
	table := "table1"
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_SELECT,
				Table: &table,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	testEtcdPut(t, "simple", "table1", map[string]interface{}{
		"key1": "val1",
		"key2": int(3),
	})
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, int(3), dump["key2"])
}

func TestTransactUpdateSimple(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"key1": "val1",
	}
	row2 := map[string]interface{}{
		"key1": "val2",
	}
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testEtcdDump(t, "simple", "table1")
	assert.Equal(t, "val2", dump["key1"])
}

func TestTransactUpdateSimple2Txn(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"key1": "val1",
		"key2": int(10),
	}
	row2 := map[string]interface{}{
		"key1": "val2",
	}
	req1 := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	/* txn 1 */
	resp, _ := testTransact(t, req1)
	assert.Nil(t, resp.Error)
	dump := testEtcdDump(t, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, float64(10), dump["key2"])
	/* txn 2 */
	resp, _ = testTransact(t, req2)
	assert.Nil(t, resp.Error)
	dump = testEtcdDump(t, "simple", "table1")
	assert.Equal(t, "val2", dump["key1"])
	assert.Equal(t, float64(10), dump["key2"])
}

func TestTransactUpdateWhere(t *testing.T) {
	table := "table1"
	uuid1 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	uuid1buf, err := json.Marshal(uuid1)
	assert.Nil(t, err)
	uuid1array := []interface{}{}
	err = json.Unmarshal(uuid1buf, &uuid1array)
	assert.Nil(t, err)

	row1 := map[string]interface{}{
		"string": "val1",
		"uuid":   &uuid1array,
	}
	req1 := &libovsdb.Transact{
		DBName: "atomic",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
		},
	}

	row2 := map[string]interface{}{
		"string": "val2",
	}

	where2 := []interface{}{
		[]interface{}{"uuid", "==", uuid1array},
	}
	req2 := &libovsdb.Transact{
		DBName: "atomic",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
				Where: &where2,
			},
		},
	}

	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)

	resp, _ := testTransact(t, req1)
	assert.Nil(t, resp.Error)
	resp, _ = testTransact(t, req2)
	assert.Nil(t, resp.Error)

	dump := testEtcdDump(t, "atomic", "table1")
	assert.Equal(t, "val2", dump["string"])
}

func TestTransactUpdateMapOk(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value2"}},
	}
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, txn := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "map", "table1")
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value2"}}, dump["string"])
}

func TestTransactUpdateMapError(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": int(2) /* error */}},
	}
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactUpdateUnmutableError(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"mutable":   1,
		"unmutable": 1,
	}
	row2 := map[string]interface{}{
		"mutable":   2,
		"unmutable": 2,
	}
	req := &libovsdb.Transact{
		DBName: "mutable",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:    OP_UPDATE,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotNil(t, resp.Error)
}

func TestTransactMutateSimple(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"key2": int(1),
	}
	mutations := []interface{}{
		[]interface{}{
			"key2",
			"+=",
			int(1),
		},
	}
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        OP_MUTATE,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testEtcdDump(t, "simple", "table1")
	assert.Equal(t, float64(2), dump["key2"])
}

func TestTransactMutateMapNamedUUID(t *testing.T) {
	namedUUID1 := "myuuid1"
	namedUUID2 := "myuuid2"

	table1 := "table1"
	table1row1 := map[string]interface{}{}

	table2 := "table2"
	table2row1 := map[string]interface{}{
		"map": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
			"uuid1": libovsdb.UUID{GoUUID: namedUUID1},
		}},
	}

	mutations := []interface{}{
		[]interface{}{
			"map",
			MT_INSERT,
			libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
				"uuid2": libovsdb.UUID{GoUUID: namedUUID2},
			}},
		},
	}

	req := &libovsdb.Transact{
		DBName: "uuid",
		Operations: []libovsdb.Operation{
			/* table1 */
			{
				Op:       OP_INSERT,
				Table:    &table1,
				Row:      &table1row1,
				UUIDName: &namedUUID1,
			},
			{
				Op:       OP_INSERT,
				Table:    &table1,
				Row:      &table1row1,
				UUIDName: &namedUUID2,
			},
			/* table2 */
			{
				Op:    OP_INSERT,
				Table: &table2,
				Row:   &table2row1,
			},
			{
				Op:        OP_MUTATE,
				Table:     &table2,
				Mutations: &mutations,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
}

func TestTransactMutateSet(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"uuid": libovsdb.OvsSet{GoSet: []interface{}{}},
	}
	uuid1 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	mutations1 := []interface{}{
		[]interface{}{
			"uuid",
			MT_INSERT,
			libovsdb.OvsSet{GoSet: []interface{}{uuid1}},
		},
	}
	uuid2 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000002"}
	mutations2 := []interface{}{
		[]interface{}{
			"uuid",
			MT_INSERT,
			libovsdb.OvsSet{GoSet: []interface{}{uuid2}},
		},
	}
	req := &libovsdb.Transact{
		DBName: "set",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        OP_MUTATE,
				Table:     &table,
				Mutations: &mutations1,
			},
			{
				Op:        OP_MUTATE,
				Table:     &table,
				Mutations: &mutations2,
			},
			{
				Op:        OP_MUTATE,
				Table:     &table,
				Mutations: &mutations1,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testEtcdDump(t, "set", "table1")
	expected := []interface{}{
		"set",
		[]interface{}{
			[]interface{}{"uuid", "00000000-0000-0000-0000-000000000001"},
			[]interface{}{"uuid", "00000000-0000-0000-0000-000000000002"},
		},
	}
	assert.Equal(t, expected, dump["uuid"])
}

func TestTransactMutateUnmutableError(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"mutable":   int(1),
		"unmutable": int(1),
	}
	mutations := []interface{}{
		[]interface{}{
			"mutable",
			"+=",
			int(1),
		},
		[]interface{}{
			"unmutable",
			"+=",
			int(1),
		},
	}
	req := &libovsdb.Transact{
		DBName: "mutable",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: &table,
				Row:   &row,
			},
			{
				Op:        OP_MUTATE,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotNil(t, resp.Error)
}

func TestTransactDelete(t *testing.T) {
	table := "table1"
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_DELETE,
				Table: &table,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	testEtcdPut(t, "simple", "table1", map[string]interface{}{
		"key1": "val1",
		"key2": int(2),
	})
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
	dump := testEtcdDump(t, "simple", "table1")
	_, ok := dump["key1"]
	assert.False(t, ok)
}

func TestTransactWaitSimpleEQ(t *testing.T) {
	table := "table1"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1a",
		},
	}
	until := FN_EQ
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
			{
				Op:      OP_WAIT,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
}

func TestTransactWaitSimpleEQColumnsNil(t *testing.T) {
	table := "table1"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	rows := []map[string]interface{}{
		{
			"key1": "val1a",
		},
	}
	until := FN_EQ
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
			{
				Op:      OP_WAIT,
				Table:   &table,
				Rows:    &rows,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
}

func TestTransactWaitSimpleEQRowsEmpty(t *testing.T) {
	table := "table1"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	rows := []map[string]interface{}{}
	columns := []string{"key1"}
	until := FN_EQ
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
			{
				Op:      OP_WAIT,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
}

func TestTransactWaitSimpleNE(t *testing.T) {
	table := "table1"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1c",
		},
	}
	until := FN_NE
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
			{
				Op:      OP_WAIT,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
}

func TestTransactWaitSimpleEQError(t *testing.T) {
	table := "table1"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1b",
		},
	}
	until := FN_EQ
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      OP_WAIT,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotNil(t, resp.Error)
	assert.Equal(t, E_TIMEOUT, *resp.Error)
}

func TestTransactWaitSimpleNEError(t *testing.T) {
	table := "table1"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1a",
		},
	}
	until := FN_NE
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      OP_WAIT,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.NotNil(t, resp.Error)
	assert.Equal(t, E_TIMEOUT, *resp.Error)
}

func testColumnDefault(t *testing.T, from interface{}) interface{} {
	buf, err := json.Marshal(from)
	assert.Nil(t, err)
	to := []interface{}{}
	err = json.Unmarshal(buf, &to)
	assert.Nil(t, err)
	return to
}

func TestTransactWaitMapEQ(t *testing.T) {
	table := "table1"
	timeout := 0
	actual1 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}
	expected1 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1"}}
	row1 := map[string]interface{}{
		"string": testColumnDefault(t, actual1),
	}
	columns := []string{"string"}
	rows := []map[string]interface{}{
		{
			"string": testColumnDefault(t, expected1),
		},
	}
	until := FN_EQ
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_INSERT,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      OP_WAIT,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
}

func TestTransactCommit(t *testing.T) {
	durable := true
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_COMMIT,
				Durable: &durable,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	resp, _ := testTransact(t, req)
	assert.NotNil(t, resp.Error)
}

func TestTransactAbort(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op: OP_ABORT,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	resp, _ := testTransact(t, req)
	assert.NotNil(t, resp.Error)
}

func TestTransactComment(t *testing.T) {
	comment := "ovs-vsctl add-br br0"
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_COMMENT,
				Comment: &comment,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req)
	assert.Nil(t, resp.Error)
}

func TestTransactAssert(t *testing.T) {
}
