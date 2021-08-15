package ovsdb

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

func TestMain(m *testing.M) {
	fs := flag.NewFlagSet("fs", flag.PanicOnError)
	klog.InitFlags(fs)
	fs.Set("v", "10")
	flag.Parse()
	testSchemaSimple.AddInternalsColumns()
	testSchemaMutable.AddInternalsColumns()
	testSchemaSet.AddInternalsColumns()
	testSchemaUUID.AddInternalsColumns()
	testSchemaMap.AddInternalsColumns()
	testSchemaAtomic.AddInternalsColumns()
	testSchemaEnum.AddInternalsColumns()
	common.SetPrefix("ovsdb/nb")
	os.Exit(m.Run())
}

var testSchemaSimple = &libovsdb.DatabaseSchema{
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

var testSchemaAtomic = &libovsdb.DatabaseSchema{
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

var testSchemaMutable = &libovsdb.DatabaseSchema{
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

var testSchemaEnum = &libovsdb.DatabaseSchema{
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

var testSchemaSet = &libovsdb.DatabaseSchema{
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
							Type: libovsdb.TypeInteger,
						},
						Max: 3,
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
						Min: 0,
						Max: libovsdb.Unlimited,
					},
				},
				"set": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
						},
						Min: 0,
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

func testEtcdPut(t *testing.T, dbname, table string, uuid string, row map[string]interface{}) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	if uuid == "" {
		uuid = common.GenerateUUID()
	}
	key := common.NewDataKey(dbname, table, uuid)
	setRowUUID(&row, key.UUID)
	val, err := makeValue(&row)
	assert.Nil(t, err)
	_, err = cli.Put(ctx, key.String(), val)
	assert.Nil(t, err)
}

func testEtcdGetComment(t *testing.T, dbName, comment string) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	key := common.NewCommentTableKey(dbName)
	response, err := cli.Get(ctx, key.String(), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	assert.Nil(t, err)
	for _, kv := range response.Kvs {
		if string(kv.Value) == comment {
			return
		}
	}
	assert.True(t, false, "Comment not found")
}

func testTransact(t *testing.T, req *libovsdb.Transact, schema *libovsdb.DatabaseSchema, expCacheElements int) *libovsdb.TransactResponse {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer cli.Close()
	cache := cache{}
	cache.addDatabaseCache(schema.Name, cli)
	dbCache := cache.getDatabase(schema.Name)
	if expCacheElements > -1 {
		var elements int
		for i := 0; i < 5; i++ {
			elements = (&cache).size()
			if elements == expCacheElements {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		assert.Equal(t, expCacheElements, elements)
	}
	txn, err := NewTransaction(context.Background(), cli, req, dbCache, schema, klogr.New())
	assert.Nil(t, err)
	txn.Commit()
	return &txn.response
}

func TestTransactInsertSimple(t *testing.T) {
	table := "table1"
	dbName := "simple"

	row := map[string]interface{}{
		"key1": "val1",
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 1, 0, "")

	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
}

func testTransactInsertSimpleScale(t *testing.T, n int) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	op := libovsdb.Operation{
		Op:    libovsdb.OperationInsert,
		Table: &table,
		Row:   &row,
	}

	req := &libovsdb.Transact{
		DBName:     dbName,
		Operations: []libovsdb.Operation{},
	}
	for i := 0; i < n; i++ {
		req.Operations = append(req.Operations, op)
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	assert.Equal(t, n, len(resp.Result))
	for i := 0; i < n; i++ {
		validateInsertResult(t, resp, n, i, "")
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, n)
	validateSelectResult(t, resp, 1, 0, n)
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
XXX: 1. use etcdTrx version 3.4.x and above
XXX: 2. configure in /etc/default/etcdTrx:
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
	dbName := "simple"
	uuid := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	row := map[string]interface{}{
		"key1": "val1",
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
				UUID:  &uuid,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 1, 0, uuid.GoUUID)

	// select check
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
}

func validateSelectResult(t *testing.T, resp *libovsdb.TransactResponse, results, checkedOperation, expectedRows int) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].Details)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.NotNil(t, resp.Result[checkedOperation].Rows)
	assert.Equal(t, expectedRows, len(*resp.Result[checkedOperation].Rows))
}

func validateEmptyResult(t *testing.T, resp *libovsdb.TransactResponse, results int, checkedOperation int) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].Details)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.Nil(t, resp.Result[checkedOperation].Rows)
}

func validateOperationError(t *testing.T, resp *libovsdb.TransactResponse, results int, checkedOperation int, errorMsg string) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.NotNil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.Nil(t, resp.Result[checkedOperation].Rows)
	if errorMsg != "" {
		assert.Equal(t, errorMsg, *resp.Result[0].Error)
	}
}

func validateUpdateResult(t *testing.T, resp *libovsdb.TransactResponse, results, checkedOperation, expectedCount int) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.NotNil(t, resp.Result[checkedOperation].Count)
	assert.Equal(t, expectedCount, *resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].Details)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.Nil(t, resp.Result[checkedOperation].Rows)
}

func validateInsertResult(t *testing.T, resp *libovsdb.TransactResponse, results int, checkedOperation int, expectedUUID string) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].Details)
	assert.Nil(t, resp.Result[checkedOperation].Rows)
	if resp.Result[checkedOperation].Error == nil {
		assert.NotNil(t, resp.Result[checkedOperation].UUID)
		if expectedUUID != "" {
			assert.Equal(t, expectedUUID, resp.Result[checkedOperation].UUID.GoUUID)
		}
	}

}

func TestTransactInsertSimpleWithUUIDName(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuidName := "myuuid"
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, -1)
	validateInsertResult(t, resp, 1, 0, "")
	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
}

func TestTransactInsertSimpleWithUUIDNameDupError(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuidName := "myuuid"
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 2, 0, "")
	assert.Nil(t, resp.Result[1].Count)
	assert.NotNil(t, resp.Result[1].Error)
	assert.Nil(t, resp.Result[1].Rows)
}

func TestTransactAtomicInsertNamedUUID(t *testing.T) {
	dbName := "atomic"
	table := "table1"
	uuidName1 := libovsdb.UUID{GoUUID: "myuuid1"}
	uuid1 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}

	row1 := map[string]interface{}{
		"string": "val1",
	}
	row2 := map[string]interface{}{
		"uuid": uuidName1,
	}

	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row1,
				UUID:     &uuid1,
				UUIDName: &uuidName1.GoUUID,
			},
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}

	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaAtomic, 0)
	validateInsertResult(t, resp, 2, 0, uuid1.GoUUID)
	validateUpdateResult(t, resp, 2, 1, 1)

	//validate new row
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, uuid1}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaAtomic, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	testSchemaAtomic.Unmarshal(table, &irow)
	assert.Equal(t, uuid1, irow["uuid"])
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
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
				UUID:  &uuid1,
			},
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
				Where: &where2,
			},
		},
	}

	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaAtomic, -1)
	assert.Nil(t, resp.Error)

	// FIXME
	//dump := testEtcdDump(t, "atomic", "table1")
	//assert.Equal(t, []interface{}{"uuid", uuid1.GoUUID}, dump["_uuid"])
}

func TestTransactInsertEnumOk(t *testing.T) {
	dbName := "enum"
	table := "table1"
	row := map[string]interface{}{
		"color": "red",
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaEnum, 0)
	validateInsertResult(t, resp, 1, 0, "")

	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaEnum, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "red", row["color"])
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
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaEnum, -1)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactInsertSetOk(t *testing.T) {
	dbName := "set"
	table := "table1"
	dbSchems := testSchemaSet
	set := libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}}
	row := map[string]interface{}{"string": set}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, dbSchems, 0)
	validateInsertResult(t, resp, 1, 0, "")

	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, dbSchems, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	dbSchems.Unmarshal(table, &row)
	assert.Equal(t, set, row["string"])
}

func TestTransactInsertSetError(t *testing.T) {
	dbName := "set"
	table := "table1"
	dbSchems := testSchemaSet
	row := map[string]interface{}{
		"string": libovsdb.OvsSet{GoSet: []interface{}{1, 2, 3}},
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, dbSchems, 0)
	validateOperationError(t, resp, 1, 0, E_CONSTRAINT_VIOLATION)
}

func TestTransactUpdateSimple(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row1 := map[string]interface{}{
		"key1": "val1",
	}
	row2 := map[string]interface{}{
		"key1": "val2",
	}
	row3 := map[string]interface{}{
		"key1": "val3",
	}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{Op: libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
			{Op: libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row3,
			},
		},
	}
	testEtcdCleanup(t)
	// insert a row
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 3, 0, "")
	validateUpdateResult(t, resp, 3, 1, 1)
	validateUpdateResult(t, resp, 3, 2, 1)

	// check the updated value
	req3 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req3, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val3", row["key1"])
}

func TestTransactUpdateMT(t *testing.T) {
	table := "table1"
	dbName := "simple"
	n := 5
	rowOrg := map[string]interface{}{
		"key1": "val1",
	}
	rowNew := map[string]interface{}{
		"key1": "val2",
	}

	uuids := make([]libovsdb.UUID, n)
	for i := 0; i < n; i++ {
		uuids[i] = libovsdb.UUID{GoUUID: common.GenerateUUID()}
	}

	ops := make([]libovsdb.Operation, n)
	for i := 0; i < n; i++ {
		ops[i] = libovsdb.Operation{
			Op:    libovsdb.OperationInsert,
			Table: &table,
			Row:   &rowOrg,
			UUID:  &uuids[i],
		}
	}

	req := &libovsdb.Transact{
		DBName:     dbName,
		Operations: ops,
	}
	testEtcdCleanup(t)
	// insert a row
	resp := testTransact(t, req, testSchemaSimple, 0)
	for i := 0; i < n; i++ {
		validateInsertResult(t, resp, n, i, uuids[i].GoUUID)
	}

	for i := 0; i < n; i++ {
		ops[i] = libovsdb.Operation{
			Op:    libovsdb.OperationUpdate,
			Table: &table,
			Row:   &rowNew,
			Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, uuids[i]}},
		}
	}
	req = &libovsdb.Transact{
		DBName:     dbName,
		Operations: ops,
	}
	resp = testTransact(t, req, testSchemaSimple, n)
	for i := 0; i < n; i++ {
		validateUpdateResult(t, resp, n, i, 1)
	}

	// check the updated value
	for i := 0; i < n; i++ {
		ops[i] = libovsdb.Operation{
			Op:    libovsdb.OperationSelect,
			Table: &table,
			Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, uuids[i]}},
		}
	}
	req = &libovsdb.Transact{
		DBName:     dbName,
		Operations: ops,
	}
	resp = testTransact(t, req, testSchemaSimple, n)
	for i := 0; i < n; i++ {
		validateSelectResult(t, resp, n, i, 1)
		row := (*resp.Result[0].Rows)[0]
		assert.Equal(t, "val2", row["key1"])
	}
}

func TestTransactUpdateMapOk(t *testing.T) {
	table := "table1"
	dbName := "map"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
			"key1": "value1a",
			"key2": "value2",
		}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1b"}},
	}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaMap, 0)
	assert.Nil(t, resp.Error)
	uuid := resp.Result[0].UUID

	// update the row
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaMap, 1)
	validateUpdateResult(t, resp, 1, 0, 1)

	// check the updated value
	req3 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req3, testSchemaMap, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	testSchemaMap.Unmarshal(table, &irow)
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1b", "key2": "value2"}}, irow["string"])
}

/*
func TestTransactUpdateMap2Txn(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
			"key1": "value1a",
			"key2": "value2",
		}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1b"}},
	}
	req1 := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	testEtcdCleanup(t)
	/ * txn 1 * /
	resp, txn := testTransact(t, req1, testSchemaMap, -1)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "map", "table1")
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
		"key1": "value1a",
		"key2": "value2",
	}}, dump["string"])

	/ * txn 2 * /
	resp, txn = testTransact(t, req2, testSchemaMap, -1)
	assert.Nil(t, resp.Error)
	dump = testTransactDump(t, txn, "map", "table1")
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
		"key1": "value1b",
		"key2": "value2",
	}}, dump["string"])
}
*/

/*
func TestTransactUpdateMapError(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": int(2) / * error * /}},
	}
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req, testSchemaMap, -1)
	assert.NotEqual(t, "", resp.Error)
}
*/

func TestTransactUpdateUnmutableError(t *testing.T) {
	table := "table1"
	dbName := "mutable"
	row1 := map[string]interface{}{
		"mutable":   1,
		"unmutable": 1,
	}
	row2 := map[string]interface{}{
		"mutable":   2,
		"unmutable": 2,
	}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaMutable, 0)
	assert.Nil(t, resp.Error)
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	resp = testTransact(t, req2, testSchemaMutable, 1)
	validateOperationError(t, resp, 1, 0, E_CONSTRAINT_VIOLATION)
}

func TestTransactMutateSimple(t *testing.T) {
	table := "table1"
	dbName := "simple"
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
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 4, 0, "")
	validateUpdateResult(t, resp, 4, 1, 1)
	validateUpdateResult(t, resp, 4, 2, 1)
	validateUpdateResult(t, resp, 4, 3, 1)

	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	assert.EqualValues(t, 4, row["key2"])
}

func TestTransactMutateSetUUID(t *testing.T) {
	table := "table2"
	dbName := "uuid"
	dbSchems := testSchemaUUID
	uuid1 := "00000000-0000-0000-0000-000000000001"
	uuid2 := "00000000-0000-0000-0000-000000000002"
	uuidName := "rowdea9b92e_4b83_4fdc_b552_8ec4b9d3581f"
	goUUID := libovsdb.UUID{GoUUID: uuidName}
	table2row1 := map[string]interface{}{
		"set": libovsdb.OvsSet{GoSet: []interface{}{
			libovsdb.UUID{GoUUID: uuid1},
		}},
	}

	mutations := []interface{}{
		[]interface{}{
			"set",
			MT_DELETE,
			libovsdb.OvsSet{GoSet: []interface{}{
				libovsdb.UUID{GoUUID: uuid1},
			}},
		},
		[]interface{}{
			"set",
			MT_INSERT,
			libovsdb.OvsSet{GoSet: []interface{}{
				libovsdb.UUID{GoUUID: uuid2},
			}},
		},
	}

	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &table2row1,
				UUIDName: &uuidName,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
				Where:     &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, goUUID}},
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, dbSchems, 0)
	validateInsertResult(t, resp, 2, 0, "")
	validateUpdateResult(t, resp, 2, 1, 1)

	req3 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req3, dbSchems, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	dbSchems.Unmarshal(table, &irow)
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{libovsdb.UUID{GoUUID: uuid2}}}, irow["set"])
}

func TestTransactMutateMap(t *testing.T) {
	table := "table1"
	dbName := "map"
	dbSchems := testSchemaMap

	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
			"key1": "value1",
			"key2": "value2",
		}},
	}

	mutations := []interface{}{
		[]interface{}{
			"string",
			MT_INSERT,
			libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
				"key2": "valueNew1",
				"key3": "value3",
			}},
		},
		[]interface{}{
			"string",
			MT_INSERT,
			libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
				"key2": "valueNew2",
				"key4": "value4",
			}},
		},
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaMap, 0)
	validateInsertResult(t, resp, 2, 0, "")
	validateUpdateResult(t, resp, 2, 1, 1)

	reqS := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, reqS, dbSchems, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	dbSchems.Unmarshal(table, &irow)
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	},
	}, irow["string"])

	mutations = []interface{}{
		[]interface{}{
			"string",
			MT_DELETE,
			libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
				"key1": "value2", // different value, should not be removed
				"key2": "value2",
			}},
		},
		[]interface{}{
			"string",
			MT_DELETE,
			libovsdb.OvsSet{GoSet: []interface{}{"key3", "key4", "key5"}},
		},
	}
	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	resp = testTransact(t, req, dbSchems, 1)
	validateUpdateResult(t, resp, 1, 0, 1)
	resp = testTransact(t, reqS, dbSchems, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	irow = row
	dbSchems.Unmarshal(table, &irow)
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}}, irow["string"])
}

func TestTransactInsertTwoNamedUUID(t *testing.T) {
	table := "table1"
	dbName := "uuid"
	dbSchems := testSchemaUUID

	uuidName := "rowdea9b92e_4b83_4fdc_b552_8ec4b9d3581f"
	uuid1 := "00000000-0000-0000-0000-000000000001"
	goUUID1 := libovsdb.UUID{GoUUID: uuid1}
	row1 := map[string]interface{}{"uuid": libovsdb.UUID{GoUUID: uuidName}}
	row2 := map[string]interface{}{"uuid": libovsdb.UUID{GoUUID: uuid1}}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
				UUID:  &goUUID1,
			},
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row2,
				UUIDName: &uuidName,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, dbSchems, 0)
	validateInsertResult(t, resp, 2, 0, uuid1)
	validateInsertResult(t, resp, 2, 1, "")
	uuid := resp.Result[1].UUID.GoUUID

	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, goUUID1}},
			},
		},
	}
	resp = testTransact(t, req2, dbSchems, 2)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	dbSchems.Unmarshal(table, &irow)
	assert.Equal(t, libovsdb.UUID{GoUUID: uuid}, irow["uuid"])
}

func TestTransactMutateSet(t *testing.T) {
	dbName := "set"
	table := "table1"
	dbSchemas := testSchemaSet
	row1 := map[string]interface{}{
		"string":  libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}},
		"integer": libovsdb.OvsSet{GoSet: []interface{}{1, 2, 3}},
	}
	mutations1 := []interface{}{
		[]interface{}{
			"string",
			MT_INSERT,
			libovsdb.OvsSet{GoSet: []interface{}{"c", "d"}},
		},
	}
	mutations2 := []interface{}{
		[]interface{}{
			"integer",
			MT_PRODUCT,
			2,
		},
	}
	mutations3 := []interface{}{
		[]interface{}{
			"string",
			MT_DELETE,
			libovsdb.OvsSet{GoSet: []interface{}{"b", "d", "e"}},
		},
		[]interface{}{
			"integer",
			MT_DELETE,
			libovsdb.OvsSet{GoSet: []interface{}{4, 7}},
		},
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations2,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations3,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, dbSchemas, 0)
	validateInsertResult(t, resp, 4, 0, "")
	validateUpdateResult(t, resp, 4, 1, 1)
	validateUpdateResult(t, resp, 4, 2, 1)
	validateUpdateResult(t, resp, 4, 3, 1)

	// get the value
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req2, dbSchemas, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	dbSchemas.Unmarshal(table, &irow)
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{"a", "c"}}, irow["string"])
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{2, 6}}, irow["integer"])
}

/*
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
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req, testSchemaMutable, -1)
	assert.NotNil(t, resp.Error)
}

*/
func TestTransactDelete(t *testing.T) {
	table := "table1"
	dbName := "simple"
	uuid := common.GenerateUUID()
	goUUID := libovsdb.UUID{GoUUID: uuid}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, goUUID}},
			},
		},
	}
	testEtcdCleanup(t)
	testEtcdPut(t, dbName, table, "", map[string]interface{}{
		"key1": "val1",
		"key2": int(3),
	})

	testEtcdPut(t, dbName, table, uuid, map[string]interface{}{
		"key3": "val1",
		"key4": int(3),
	})
	resp := testTransact(t, req, testSchemaSimple, 2)
	validateUpdateResult(t, resp, 1, 0, 1)
	// check select
	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, goUUID}},
			},
		},
	}
	resp = testTransact(t, req, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 0)
}

func TestTransactWaitSimpleEQ(t *testing.T) {
	table := "table1"
	dbName := "simple"
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
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

// FIXME insert + timeout
func TestTransactWaitSimpleEQColumnsNil(t *testing.T) {
	table := "table1"
	dbName := "simple"
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
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

func TestTransactWaitSimpleEQRowsEmpty(t *testing.T) {
	table := "table1"
	dbName := "simple"
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
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

// FIXME timeouts
func TestTransactWaitSimpleNE(t *testing.T) {
	table := "table1"
	dbName := "simple"
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
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 2, 0, "")
	validateInsertResult(t, resp, 2, 1, "")
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

func TestTransactWaitSimpleEQError(t *testing.T) {
	table := "table1"
	dbName := "simple"
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
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateOperationError(t, resp, 1, 0, E_TIMEOUT)
}

func TestTransactWaitSimpleNEError(t *testing.T) {
	table := "table1"
	dbName := "simple"
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
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 1, 0, "")
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateOperationError(t, resp, 1, 0, E_TIMEOUT)
}

func testColumnDefault(t *testing.T, from interface{}) interface{} {
	buf, err := json.Marshal(from)
	assert.Nil(t, err)
	to := []interface{}{}
	err = json.Unmarshal(buf, &to)
	assert.Nil(t, err)
	return to
}

// FIXME
func TestTransactWaitMapEQ(t *testing.T) {
	table := "table1"
	timeout := 0
	actual1 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "a,b,c"}}
	//expected1 := actual1
	row1 := map[string]interface{}{
		"string": testColumnDefault(t, actual1),
	}
	columns := []string{"string"}
	rows := []map[string]interface{}{row1}

	until := FN_EQ
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaMap, -1)
	validateInsertResult(t, resp, 2, 0, "")
	//validateEmptyResult(t, resp, 2, 1)
}

func TestTransactCommit(t *testing.T) {
	durable := true
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationCommit,
				Durable: &durable,
			},
		},
	}
	resp := testTransact(t, req, testSchemaSimple, -1)
	validateOperationError(t, resp, 1, 0, E_NOT_SUPPORTED)
}

func TestTransactAbort(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op: libovsdb.OperationAbort,
			},
		},
	}
	resp := testTransact(t, req, testSchemaSimple, -1)
	validateOperationError(t, resp, 1, 0, E_ABORTED)
}

func TestTransactSelect(t *testing.T) {
	table := "table1"
	dbName := "simple"
	uuid := common.GenerateUUID()
	goUUID := libovsdb.UUID{GoUUID: uuid}

	simpleReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	UUIDReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EQ, goUUID}},
			},
		},
	}
	noneUUIDReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.COL_UUID, FN_EX, goUUID}},
			},
		},
	}

	testEtcdCleanup(t)
	testEtcdPut(t, dbName, table, "", map[string]interface{}{
		"key1": "val1",
		"key2": int(3),
	})

	testEtcdPut(t, dbName, table, uuid, map[string]interface{}{
		"key3": "val1",
		"key4": int(3),
	})
	checkUUIDRow := func(row libovsdb.ResultRow) {
		assert.Equal(t, "val1", row["key3"])
		assert.EqualValues(t, int(3), row["key4"])
	}
	checkNoneUUIDRow := func(row libovsdb.ResultRow) {
		assert.Equal(t, "val1", row["key1"])
		assert.EqualValues(t, int(3), row["key2"])
	}

	resp := testTransact(t, simpleReq, testSchemaSimple, 2)
	validateSelectResult(t, resp, 1, 0, 2)
	for i := 0; i < 2; i++ {
		row := (*resp.Result[0].Rows)[i]
		iUUID, err := libovsdb.UnmarshalUUID(row[libovsdb.COL_UUID])
		assert.Nil(t, err)
		goUUID, ok := iUUID.(libovsdb.UUID)
		assert.True(t, ok)
		if goUUID.GoUUID == uuid {
			checkUUIDRow(row)
		} else {
			checkNoneUUIDRow(row)
		}
	}
	// where [[_uuid == uuid]]
	resp = testTransact(t, UUIDReq, testSchemaSimple, 2)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	checkUUIDRow(row)
	// where [[_uuid exclude uuid]]
	resp = testTransact(t, noneUUIDReq, testSchemaSimple, 2)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	checkNoneUUIDRow(row)
}

func TestTransactSelectAndComment(t *testing.T) {
	comment := "select test data"
	table := "table1"
	dbName := "simple"

	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
			{
				Op:      libovsdb.OperationComment,
				Comment: &comment,
			},
		},
	}
	testEtcdCleanup(t)
	testEtcdPut(t, dbName, table, "", map[string]interface{}{
		"key1": "val1",
		"key2": int(3),
	})
	resp := testTransact(t, req, testSchemaSimple, 1)
	validateSelectResult(t, resp, 2, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
	assert.EqualValues(t, int(3), row["key2"])

	validateEmptyResult(t, resp, 2, 1)
	testEtcdGetComment(t, req.DBName, comment)
}

func TestTransactComment(t *testing.T) {
	comment := "ovs-vsctl add-br br0"
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationComment,
				Comment: &comment,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, -1)
	assert.Nil(t, resp.Error)
	testEtcdGetComment(t, req.DBName, comment)
}

func TestTransactAssert(t *testing.T) {
}

func TestEqualMap(t *testing.T) {
	expectedValue := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"max_tunid": "16711680", "northd_internal_version": "21.03.1-20.16.1-56.0",
		"svc_monitor_mac": "4e:15:a4:a3:c8:7c", "use_logical_dp_groups": "true", "mac_prefix": "26:20:60"}}

	actualValue := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"svc_monitor_mac": "4e:15:a4:a3:c8:7c",
		"use_logical_dp_groups": "true", "mac_prefix": "26:20:60", "max_tunid": "16711680", "northd_internal_version": "21.03.1-20.16.1-56.0"}}

	colSchema, err := testSchemaMap.LookupColumn("table1", "string")
	assert.Nil(t, err)
	retValue := isEqualColumn(colSchema, expectedValue, actualValue)
	assert.True(t, retValue)
}
