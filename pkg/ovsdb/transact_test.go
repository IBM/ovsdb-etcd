package ovsdb

import (
	"context"
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
					Type:    libovsdb.TypeString,
					Mutable: true,
				},
				"key2": {
					Type:    libovsdb.TypeInteger,
					Mutable: true,
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
					Type:    libovsdb.TypeInteger,
					Mutable: true,
				},
				"unmutable": {
					Type:    libovsdb.TypeInteger,
					Mutable: false,
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
				"boolean": {
					Type: libovsdb.TypeBoolean,
				},
				"integer": {
					Type: libovsdb.TypeInteger,
				},
				"real": {
					Type: libovsdb.TypeInteger,
				},
				"uuid": {
					Type: libovsdb.TypeInteger,
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
							Enum: []interface{}{"red", "green", "blue"},
						},
					},
					Mutable: true,
				},
				"animal": {
					Type: libovsdb.TypeEnum,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
							Enum: []interface{}{"dog", "cat", "mouse"},
						},
					},
					Mutable: true,
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
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Max: libovsdb.Unlimited,
						Min: 0,
					},
					Mutable: true,
				},
				"integer": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Max: 2,
						Min: 0,
					},
					Mutable: true,
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
						Min: 1,
						Max: 1,
					},
					Mutable: true,
				},
			},
		},
	},
}

func testEtcdNewCli() (*clientv3.Client, error) {
	endpoints := []string{"http://127.0.0.1:2379"}
	return NewEtcdClient(endpoints)
}

func testEtcdCleanup(t *testing.T, dbname, table string) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	key := common.NewTableKey(dbname, table)
	_, err = cli.Delete(ctx, key.TableKeyString(), clientv3.WithPrefix())
	assert.Nil(t, err)
}

func testEtcdCleanupComment(t *testing.T, dbname string) {
	testEtcdCleanup(t, dbname, "_comment")
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
			if k == COL_UUID || k == COL_VERSION {
				continue
			}
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
	txn := NewTransaction(cli, req)
	txn.AddSchema(testSchemaSimple)
	txn.AddSchema(testSchemaMutable)
	txn.AddSchema(testSchemaAtomic)
	txn.AddSchema(testSchemaEnum)
	txn.AddSchema(testSchemaSet)
	txn.AddSchema(testSchemaMap)
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
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"key1": "val1",
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "simple", "table1")
	resp, txn := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testTransactDump(t, txn, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, int(0), dump["key2"])
}

func TestTransactInsertEnumOk(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "enum",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"color": "red",
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "enum", "table1")
	resp, txn := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testTransactDump(t, txn, "enum", "table1")
	assert.Equal(t, "red", dump["color"])
}

func TestTransactInsertEnumError(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "enum",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"animal": "red",
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "enum", "table1")
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactInsertSetOk(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "set",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"string": libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}},
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "set", "table1")
	resp, txn := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testTransactDump(t, txn, "set", "table1")
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}}, dump["string"])
}

func TestTransactInsertSetError(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "set",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"string": libovsdb.OvsSet{GoSet: []interface{}{1, 2, 3}},
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "set", "table1")
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactSelect(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_SELECT,
				Table: "table1",
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "simple", "table1")
	testEtcdPut(t, "simple", "table1", map[string]interface{}{
		"key1": "val1",
		"key2": int(3),
	})
	resp, txn := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testTransactDump(t, txn, "simple", "table1")
	assert.Equal(t, "val1", dump["key1"])
	assert.Equal(t, int(3), dump["key2"])
}

func TestTransactUpdateSimple(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"key1": "val1",
				},
			},
			{
				Op:    OP_UPDATE,
				Table: "table1",
				Row: map[string]interface{}{
					"key1": "val2",
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "simple", "table1")
	resp, _ := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, "simple", "table1")
	assert.Equal(t, "val2", dump["key1"])
}

func TestTransactUpdateMapOk(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}},
				},
			},
			{
				Op:    OP_UPDATE,
				Table: "table1",
				Row: map[string]interface{}{
					"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value2"}},
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "map", "table1")
	resp, txn := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testTransactDump(t, txn, "map", "table1")
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value2"}}, dump["string"])
}

func TestTransactUpdateMapError(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}},
				},
			},
			{
				Op:    OP_UPDATE,
				Table: "table1",
				Row: map[string]interface{}{
					"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": int(2) /* error */}},
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "map", "table1")
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactUpdateUnmutableError(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "mutable",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"mutable":   1,
					"unmutable": 1,
				},
			},
			{
				Op:    OP_UPDATE,
				Table: "table1",
				Row: map[string]interface{}{
					"mutable":   2,
					"unmutable": 2,
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "mutable", "table1")
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactMutateSimple(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"key2": int(1),
				},
			},
			{
				Op:    OP_MUTATE,
				Table: "table1",
				Mutations: []interface{}{
					[]interface{}{
						"key2",
						"+=",
						int(1),
					},
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "simple", "table1")
	resp, _ := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, "simple", "table1")
	assert.Equal(t, float64(2), dump["key2"])
}

func TestTransactMutateSimpleValidationError(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"key2": int(1),
				},
			},
			{
				Op:    OP_MUTATE,
				Table: "table1",
				Mutations: []interface{}{
					[]interface{}{
						"key2",
						"+=",
						float64(1),
					},
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "simple", "table1")
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactMutateUnmutableError(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "mutable",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: "table1",
				Row: map[string]interface{}{
					"mutable":   int(1),
					"unmutable": int(1),
				},
			},
			{
				Op:    OP_MUTATE,
				Table: "table1",
				Mutations: []interface{}{
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
				},
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "mutable", "table1")
	resp, _ := testTransact(t, req)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactDelete(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_DELETE,
				Table: "table1",
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanup(t, "simple", "table1")
	testEtcdPut(t, "simple", "table1", map[string]interface{}{
		"key1": "val1",
		"key2": int(2),
	})
	resp, _ := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, "simple", "table1")
	_, ok := dump["key1"]
	assert.False(t, ok)
}

func TestTransactWait(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:    OP_WAIT,
				Table: "table1",
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	resp, _ := testTransact(t, req)
	assert.True(t, "" != resp.Error)
}

func TestTransactCommit(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_COMMIT,
				Durable: true,
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	resp, _ := testTransact(t, req)
	assert.True(t, "" != resp.Error)
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
	assert.True(t, "" != resp.Error)
}

func TestTransactComment(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      OP_COMMENT,
				Comment: "ovs-vsctl add-br br0",
			},
		},
	}
	common.SetPrefix("ovsdb/nb")
	testEtcdCleanupComment(t, "simple")
	resp, _ := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
}

func TestTransactAssert(t *testing.T) {
}
