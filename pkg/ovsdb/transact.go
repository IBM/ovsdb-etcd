package ovsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.etcd.io/etcd/client/v3/concurrency"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/jinzhu/copier"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

const (
	/* ovsdb operations */
	E_DUP_UUIDNAME         = "duplicate uuid-name"
	E_CONSTRAINT_VIOLATION = "constraint violation"
	E_DOMAIN_ERROR         = "domain error"
	E_RANGE_ERROR          = "range error"
	E_TIMEOUT              = "timed out"
	E_NOT_SUPPORTED        = "not supported"
	E_ABORTED              = "aborted"
	E_NOT_OWNER            = "not owner"

	/* ovsdb transaction */
	E_INTEGRITY_VIOLATION = "referential integrity violation"
	E_RESOURCES_EXHAUSTED = "resources exhausted"
	E_IO_ERROR            = "I/O error"

	/* ovsdb extention */
	E_DUP_UUID         = "duplicate uuid"
	E_INTERNAL_ERROR   = "internal error"
	E_OVSDB_ERROR      = "ovsdb error"
	E_PERMISSION_ERROR = "permission error"
	E_SYNTAX_ERROR     = "syntax error or unknown column"
)

func isEqualSet(expected, actual interface{}) bool {
	return reflect.DeepEqual(expected, actual)
}

func isIncludesSet(expected, actual interface{}) bool {
	expectedSet := expected.(libovsdb.OvsSet)
	actualSet := actual.(libovsdb.OvsSet)
	for _, expectedVal := range expectedSet.GoSet {
		foundVal := false
		for _, actualVal := range actualSet.GoSet {
			if isEqualValue(expectedVal, actualVal) {
				foundVal = true
			}
		}
		if !foundVal {
			return false
		}
	}
	return true
}

type Alphabetic []string

func (list Alphabetic) Len() int { return len(list) }

func (list Alphabetic) Swap(i, j int) { list[i], list[j] = list[j], list[i] }

func (list Alphabetic) Less(i, j int) bool {
	var si string = list[i]
	var sj string = list[j]
	var si_lower = strings.ToLower(si)
	var sj_lower = strings.ToLower(sj)
	if si_lower == sj_lower {
		return si < sj
	}
	return si_lower < sj_lower
}

func splitAndSort(s string) string {
	list := strings.Split(s, ",")

	sort.Sort(Alphabetic(list))

	var buffer bytes.Buffer
	for _, val := range list {
		buffer.WriteString(val)
	}

	return buffer.String()
}

func splitAndSortStrings(expectedVal, actualVal *interface{}) {
	expectedValStr, expectedOK := (*expectedVal).(string)
	actualValStr, actualOK := (*actualVal).(string)
	if expectedOK && actualOK {
		(*expectedVal) = splitAndSort(expectedValStr)
		(*actualVal) = splitAndSort(actualValStr)
	}
}

func isEqualMap(expected, actual interface{}) bool {
	return reflect.DeepEqual(expected, actual)
}

func isIncludesMap(expected, actual interface{}) bool {
	expectedMap := expected.(libovsdb.OvsMap)
	actualMap := actual.(libovsdb.OvsMap)
	for key, expectedVal := range expectedMap.GoMap {
		actualVal, ok := actualMap.GoMap[key]
		if !ok {
			return false
		}

		// the following is a due to discovering that some map values
		// are a string encoding of either:
		// map: "<key1:val1>:<key2:val2>..."
		// set: "<val1>,<val2>..."
		// thus we sort before comparison
		splitAndSortStrings(&expectedVal, &actualVal)

		if !isEqualValue(expectedVal, actualVal) {
			return false
		}
	}
	return true
}

func isEqualValue(expected, actual interface{}) bool {
	return reflect.DeepEqual(expected, actual)
}

func isEqualColumn(columnSchema *libovsdb.ColumnSchema, expected, actual interface{}) bool {
	switch columnSchema.Type {
	case libovsdb.TypeSet:
		return isEqualSet(expected, actual)
	case libovsdb.TypeMap:
		return isEqualMap(expected, actual)
	default:
		return isEqualValue(expected, actual)
	}
}

func isEqualRow(txn *Transaction, tableSchema *libovsdb.TableSchema, expectedRow, actualRow *map[string]interface{}) (bool, error) {
	for column, expected := range *expectedRow {
		columnSchema, err := tableSchema.LookupColumn(column)
		if err != nil {
			err := errors.New(E_CONSTRAINT_VIOLATION)
			txn.log.Error(err, "schema doesn't contain column", "column", column)
			return false, err
		}
		actual := (*actualRow)[column]
		if !isEqualColumn(columnSchema, expected, actual) {
			return false, nil
		}

	}
	return true, nil
}

// XXX: move to libovsdb
const (
	OP_INSERT  = "insert"
	OP_SELECT  = "select"
	OP_UPDATE  = "update"
	OP_MUTATE  = "mutate"
	OP_DELETE  = "delete"
	OP_WAIT    = "wait"
	OP_COMMIT  = "commit"
	OP_ABORT   = "abort"
	OP_COMMENT = "comment"
	OP_ASSERT  = "assert"
)

// etcd doesn't allow modification of the same key in a single transaction.
// we have to validate correctness of this operation removing.
func (txn *Transaction) etcdRemoveDupThen() {
	duplicatedKeys := map[int]int{}
	prevKeyIndex := map[string]int{}
	for curr, op := range txn.etcd.Then {
		key := string(op.KeyBytes())
		prev, ok := prevKeyIndex[key]
		if ok {
			txn.log.V(6).Info("[then] removing key", "key", key, "index", prev)
			duplicatedKeys[prev] = prev
		}
		prevKeyIndex[key] = curr
	}

	newThen := []clientv3.Op{}
	for inx, op := range txn.etcd.Then {
		if _, ok := duplicatedKeys[inx]; !ok {
			newThen = append(newThen, op)
		}
	}
	txn.etcd.Then = newThen
}

func (txn *Transaction) etcdRemoveDup() {
	txn.etcdRemoveDupThen()
}

func (txn *Transaction) etcdTransaction() (*clientv3.TxnResponse, error) {
	txn.log.V(6).Info("etcd transaction", "etcd", txn.etcd.String())
	errInternal := txn.etcd.Commit()
	if errInternal != nil {
		err := errors.New(E_IO_ERROR)
		txn.log.Error(err, "etcd transaction", "err", errInternal)
		return nil, err
	}
	txn.cache.GetFromEtcd(txn.etcd.Res)

	err := txn.cache.Unmarshal(txn, txn.schemas)
	if err != nil {
		txn.log.Error(err, "cache unmarshal")
		return nil, err
	}

	err = txn.cache.Validate(txn, txn.schemas)
	if err != nil {
		xErr := errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "cache validate")
		return nil, xErr
	}

	return txn.etcd.Res, nil
}

type Cache map[string]DatabaseCache

type DatabaseCache map[string]TableCache

type TableCache map[string]*map[string]interface{}

func (c *Cache) Database(dbname string) DatabaseCache {
	db, ok := (*c)[dbname]
	if !ok {
		db = DatabaseCache{}
		(*c)[dbname] = db
	}
	return db
}

func (c *Cache) Table(dbname, table string) TableCache {
	db := c.Database(dbname)
	tb, ok := db[table]
	if !ok {
		tb = TableCache{}
		db[table] = tb
	}
	return tb
}

func (c *Cache) Row(key common.Key) *map[string]interface{} {
	tb := c.Table(key.DBName, key.TableName)
	_, ok := tb[key.UUID]
	if !ok {
		tb[key.UUID] = new(map[string]interface{})
	}
	return tb[key.UUID]
}

func (c *Cache) GetFromEtcdKV(kvs []*mvccpb.KeyValue) error {
	for _, x := range kvs {
		kv, err := NewKeyValue(x)
		if err != nil {
			return err
		}
		row := c.Row(kv.Key)
		(*row) = kv.Value
	}
	return nil
}

func (cache *Cache) GetFromEtcd(res *clientv3.TxnResponse) {
	for _, r := range res.Responses {
		switch v := r.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			cache.GetFromEtcdKV(v.ResponseRange.Kvs)
		}
	}
}

func (cache *Cache) Unmarshal(txn *Transaction, schemas libovsdb.Schemas) error {
	for database, databaseCache := range *cache {
		for table, tableCache := range databaseCache {
			for _, row := range tableCache {
				err := schemas.Unmarshal(database, table, row)
				if err != nil {
					txn.log.Error(err, "failed schema unmarshal")
					return err
				}
			}
		}
	}
	return nil
}

func (cache *Cache) Validate(txn *Transaction, schemas libovsdb.Schemas) error {
	for database, databaseCache := range *cache {
		for table, tableCache := range databaseCache {
			for _, row := range tableCache {
				err := schemas.Validate(database, table, row)
				if err != nil {
					txn.log.Error(err, "failed schema validate")
					return err
				}
			}
		}
	}
	return nil
}

type MapUUID map[string]string

func (mapUUID MapUUID) Set(txn *Transaction, uuidName, uuid string) {
	txn.log.V(6).Info("set named-uuid", "uuid-name", uuidName, "uuid", uuid)
	mapUUID[uuidName] = uuid
}

func (mapUUID MapUUID) Get(log logr.Logger, uuidName string) (string, error) {
	uuid, ok := mapUUID[uuidName]
	if !ok {
		err := errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "can't get named-uuid", "uuid-name", uuidName)
		return "", err
	}
	return uuid, nil
}

func (mapUUID MapUUID) ResolvUUID(log logr.Logger, value interface{}) (interface{}, error) {
	namedUuid, _ := value.(libovsdb.UUID)
	if namedUuid.GoUUID != "" && namedUuid.ValidateUUID() != nil {
		uuid, err := mapUUID.Get(log, namedUuid.GoUUID)
		if err != nil {
			return nil, err
		}
		value = libovsdb.UUID{GoUUID: uuid}
	}
	return value, nil
}

func (mapUUID MapUUID) ResolvSet(log logr.Logger, value interface{}) (interface{}, error) {
	oldset, _ := value.(libovsdb.OvsSet)
	newset := libovsdb.OvsSet{}
	for _, oldval := range oldset.GoSet {
		newval, err := mapUUID.ResolvUUID(log, oldval)
		if err != nil {
			return nil, err
		}
		newset.GoSet = append(newset.GoSet, newval)
	}
	return newset, nil
}

func (mapUUID MapUUID) ResolvMap(log logr.Logger, value interface{}) (interface{}, error) {
	oldmap, _ := value.(libovsdb.OvsMap)
	newmap := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}
	for key, oldval := range oldmap.GoMap {
		newval, err := mapUUID.ResolvUUID(log, oldval)
		if err != nil {
			return nil, err
		}
		newmap.GoMap[key] = newval
	}
	return newmap, nil
}

func (mapUUID MapUUID) Resolv(log logr.Logger, value interface{}) (interface{}, error) {
	switch value.(type) {
	case libovsdb.UUID:
		return mapUUID.ResolvUUID(log, value)
	case libovsdb.OvsSet:
		return mapUUID.ResolvSet(log, value)
	case libovsdb.OvsMap:
		return mapUUID.ResolvMap(log, value)
	default:
		return value, nil
	}
}

func (mapUUID MapUUID) ResolvRow(log logr.Logger, row *map[string]interface{}) error {
	for column, value := range *row {
		value, err := mapUUID.Resolv(log, value)
		if err != nil {
			return err
		}
		(*row)[column] = value
	}
	return nil
}

type Etcd struct {
	Cli  *clientv3.Client
	Ctx  context.Context
	If   []clientv3.Cmp
	Then []clientv3.Op
	Else []clientv3.Op
	Res  *clientv3.TxnResponse
}

func (etcd *Etcd) Clear() {
	etcd.If = []clientv3.Cmp{}
	etcd.Then = []clientv3.Op{}
	etcd.Else = []clientv3.Op{}
	etcd.Res = nil
}

func (etcd Etcd) String() string {
	return fmt.Sprintf("{txn-num-op=%d}", len(etcd.Then))
}

func (etcd *Etcd) Commit() error {
	res, err := etcd.Cli.Txn(etcd.Ctx).If(etcd.If...).Then(etcd.Then...).Else(etcd.Else...).Commit()
	if err != nil {
		return err
	}
	etcd.Res = res
	return nil
}

type Transaction struct {
	/* logger */
	log logr.Logger

	/* ovs */
	schemas  libovsdb.Schemas
	request  libovsdb.Transact
	response libovsdb.TransactResponse

	/* cache */
	cache   Cache
	mapUUID MapUUID

	/* etcd */
	etcd *Etcd

	/* session for tables locks */
	session *concurrency.Session
}

func NewTransaction(cli *clientv3.Client, log logr.Logger, request *libovsdb.Transact) *Transaction {
	txn := new(Transaction)
	txn.log = log.WithValues()
	txn.log.V(5).Info("new transaction", "size", len(request.Operations), "request", request)
	txn.cache = Cache{}
	txn.mapUUID = MapUUID{}
	txn.schemas = libovsdb.Schemas{}
	txn.request = *request
	txn.response.Result = make([]libovsdb.OperationResult, len(request.Operations))
	txn.etcd = new(Etcd)
	txn.etcd.Ctx = context.TODO()
	txn.etcd.Cli = cli
	return txn
}

type ovsOpCallback func(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error

var ovsOpCallbackMap = map[string][2]ovsOpCallback{
	OP_INSERT:  {preInsert, doInsert},
	OP_SELECT:  {preSelect, doSelect},
	OP_UPDATE:  {preUpdate, doUpdate},
	OP_MUTATE:  {preMutate, doMutate},
	OP_DELETE:  {preDelete, doDelete},
	OP_WAIT:    {preWait, doWait},
	OP_COMMIT:  {preCommit, doCommit},
	OP_ABORT:   {preAbort, doAbort},
	OP_COMMENT: {preComment, doComment},
	OP_ASSERT:  {preAssert, doAssert},
}

func (txn *Transaction) AddSchemaFromFile(path string) error {
	return txn.schemas.AddFromFile(path)
}

func (txn *Transaction) AddSchema(databaseSchema *libovsdb.DatabaseSchema) {
	txn.schemas.Add(databaseSchema)
}

func (txn *Transaction) Commit() (int64, error) {
	var err error

	/* verify that "select" is not intermixed with write operations */
	hasSelect := false
	hasWrite := false
Loop:
	for _, ovsOp := range txn.request.Operations {
		switch ovsOp.Op {
		case OP_SELECT:
			hasSelect = true
			if hasWrite {
				break Loop
			}
		case OP_DELETE, OP_MUTATE, OP_UPDATE, OP_INSERT:
			hasWrite = true
			if hasSelect {
				break Loop
			}
		default:
			// do nothing, operations like OP_WAIT, OP_COMMIT, OP_ABORT, OP_COMMENT, and OP_ASSERT are compatible
			// with OP_SELECT, despite that there is no sense to combine them together (e.g. OP_COMMIT)
		}
	}
	if hasSelect && hasWrite {
		err := errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "Can't mix select with other operations")
		errStr := err.Error()
		txn.response.Error = &errStr
		return -1, err
	}

	/* fetch needed data from database needed to perform the operation */
	txn.etcd.Clear()
	for i, ovsOp := range txn.request.Operations {
		err := ovsOpCallbackMap[ovsOp.Op][0](txn, &ovsOp, &txn.response.Result[i])
		if err != nil {
			errStr := err.Error()
			txn.response.Result[i].SetError(errStr)
			txn.response.Error = &errStr
			return -1, err
		}

		// TODO do we need this validation
		if err = txn.cache.Validate(txn, txn.schemas); err != nil {
			panic(fmt.Sprintf("validation of %s failed: %s", ovsOp, err.Error()))
		}
	}
	_, err = txn.etcdTransaction()
	if err != nil {
		errStr := err.Error()
		txn.response.Error = &errStr
		return -1, err
	}

	/* commit actual transactional changes to database */
	txn.etcd.Clear()
	for i, ovsOp := range txn.request.Operations {
		err = ovsOpCallbackMap[ovsOp.Op][1](txn, &ovsOp, &txn.response.Result[i])
		if err != nil {
			errStr := err.Error()
			txn.response.Result[i].SetError(errStr)
			txn.response.Error = &errStr
			return -1, err
		}

		if err = txn.cache.Validate(txn, txn.schemas); err != nil {
			panic(fmt.Sprintf("validation of %s failed: %s", ovsOp, err.Error()))
		}
	}

	txn.etcdRemoveDup()
	trResponse, err := txn.etcdTransaction()
	if err != nil {
		errStr := err.Error()
		txn.response.Error = &errStr
		return -1, err
	}

	txn.log.V(5).Info("commit transaction", "response", txn.response)
	return trResponse.Header.Revision, nil
}

func (txn *Transaction) lockTables() error {
	var tables []string
	tMap := make(map[string]bool)
	for _, op := range txn.request.Operations {
		switch op.Op {
		case OP_INSERT, OP_UPDATE, OP_MUTATE, OP_DELETE, OP_SELECT:
			if _, value := tMap[*op.Table]; !value {
				tMap[*op.Table] = true
				tables = append(tables, *op.Table)
			}
		default:
			// do nothing
		}
	}
	if len(tables) > 0 {
		// we have to sort to prevent deadlocks
		sort.Strings(tables)
		session, err := concurrency.NewSession(txn.etcd.Cli, concurrency.WithContext(txn.etcd.Ctx))
		if err != nil {
			return err
		}
		txn.session = session
		for _, table := range tables {
			key := common.NewLockKey("__" + table)
			mutex := concurrency.NewMutex(session, key.String())
			mutex.Lock(txn.etcd.Ctx)
		}
	}
	return nil
}

func (txn *Transaction) unLockTables() error {
	if txn.session != nil {
		return txn.session.Close()
	}
	return nil
}

// XXX: move to db
func makeValue(row *map[string]interface{}) (string, error) {
	b, err := json.Marshal(*row)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func setRowUUID(row *map[string]interface{}, uuid string) {
	(*row)[libovsdb.COL_UUID] = libovsdb.UUID{GoUUID: uuid}
}

func setRowVersion(row *map[string]interface{}) {
	version := common.GenerateUUID()
	(*row)[libovsdb.COL_VERSION] = libovsdb.UUID{GoUUID: version}
}

func (txn *Transaction) getUUIDIfExists(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, cond1 interface{}) (string, error) {
	var err error
	cond2, ok := cond1.([]interface{})
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed to convert condition", "condition", cond1)
		return "", err
	}
	condition, err := NewCondition(txn.log, tableSchema, mapUUID, cond2)
	if err != nil {
		txn.log.Error(err, "failed to create condition", "condition", cond2)
		return "", err
	}
	if condition.Column != libovsdb.COL_UUID {
		return "", nil
	}
	if condition.Function != FN_EQ && condition.Function != FN_IN {
		return "", nil
	}
	ovsUUID, ok := condition.Value.(libovsdb.UUID)
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed to convert condition value", "value", condition.Value)
		return "", err
	}
	err = ovsUUID.ValidateUUID()
	if err != nil {
		txn.log.Error(err, "failed uuid validation")
		return "", err
	}
	return ovsUUID.GoUUID, err
}

func (txn *Transaction) doesWhereContainCondTypeUUID(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, where *[]interface{}) (string, error) {
	var err error
	if where == nil {
		return "", nil
	}
	for _, c := range *where {
		cond, ok := c.([]interface{})
		if !ok {
			err = errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "failed to convert condition", "condition", c)
			return "", err
		}
		uuid, err := txn.getUUIDIfExists(tableSchema, mapUUID, cond)
		if err != nil {
			return "", err
		}
		if uuid != "" {
			return uuid, nil
		}
	}
	return "", nil

}

func (txn *Transaction) isRowSelectedByWhere(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, row *map[string]interface{}, where *[]interface{}) (bool, error) {
	var err error
	if where == nil {
		return true, nil
	}
	for _, c := range *where {
		cond, ok := c.([]interface{})
		if !ok {
			err = errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "failed to convert condition value", "condition", c)
			return false, err
		}
		ok, err := txn.isRowSelectedByCond(tableSchema, mapUUID, row, cond)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (txn *Transaction) isRowSelectedByCond(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, row *map[string]interface{}, cond []interface{}) (bool, error) {
	condition, err := NewCondition(txn.log, tableSchema, mapUUID, cond)
	if err != nil {
		return false, err
	}
	return condition.Compare(row)
}

func reduceRowByColumns(row *map[string]interface{}, columns *[]string) (*map[string]interface{}, error) {
	if columns == nil {
		return row, nil
	}
	newRow := map[string]interface{}{}
	for _, column := range *columns {
		newRow[column] = (*row)[column]
	}
	return &newRow, nil
}

func (txn *Transaction) RowMutate(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, oldRow *map[string]interface{}, mutations *[]interface{}) (*map[string]interface{}, error) {
	newRow := &map[string]interface{}{}
	copier.Copy(newRow, oldRow)
	err := tableSchema.Unmarshal(newRow)
	if err != nil {
		return nil, err
	}
	for _, mt := range *mutations {
		m, err := NewMutation(txn.log, tableSchema, mapUUID, mt.([]interface{}))
		if err != nil {
			return nil, err
		}
		err = m.Mutate(newRow)
		if err != nil {
			return nil, err
		}
	}
	return newRow, nil
}

func columnUpdateMap(oldValue, newValue interface{}) interface{} {
	oldMap := oldValue.(libovsdb.OvsMap)
	newMap := newValue.(libovsdb.OvsMap)
	retMap := libovsdb.OvsMap{}
	copier.Copy(&retMap, &oldMap)
	for k, v := range newMap.GoMap {
		retMap.GoMap[k] = v
	}
	return retMap
}

func columnUpdateSet(oldValue, newValue interface{}) interface{} {
	oldSet := oldValue.(libovsdb.OvsSet)
	newSet := newValue.(libovsdb.OvsSet)
	retSet := libovsdb.OvsSet{}
	copier.Copy(&retSet, &oldSet)
	for _, v := range newSet.GoSet {
		if !inSet(&oldSet, v) {
			retSet.GoSet = append(retSet.GoSet, v)
		}
	}
	return retSet
}

func columnUpdateValue(oldValue, newValue interface{}) interface{} {
	return newValue
}

func columnUpdate(columnSchema *libovsdb.ColumnSchema, oldValue, newValue interface{}) interface{} {
	switch columnSchema.Type {
	case libovsdb.TypeMap:
		return columnUpdateMap(oldValue, newValue)
	default:
		return columnUpdateValue(oldValue, newValue)
	}
}

func (txn *Transaction) RowUpdate(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, currentRow *map[string]interface{}, update *map[string]interface{}) (*map[string]interface{}, error) {
	newRow := new(map[string]interface{})
	copier.Copy(newRow, currentRow)
	err := tableSchema.Unmarshal(newRow)
	if err != nil {
		return nil, err
	}
	for column, value := range *update {
		columnSchema, err := tableSchema.LookupColumn(column)
		if err != nil {
			err = errors.New(E_CONSTRAINT_VIOLATION)
			txn.log.Error(err, "failed column schema lookup", "column", column)
			return nil, err
		}
		switch column {
		case libovsdb.COL_UUID, libovsdb.COL_VERSION:
			err = errors.New(E_CONSTRAINT_VIOLATION)
			txn.log.Error(err, "failed update of column", "column", column)
			return nil, err
		}
		if columnSchema.Mutable != nil && !*columnSchema.Mutable {
			err = errors.New(E_CONSTRAINT_VIOLATION)
			txn.log.Error(err, "failed update of unmutable column", "column", column)
			return nil, err
		}

		(*newRow)[column] = columnUpdate(columnSchema, (*newRow)[column], value)
	}
	return newRow, nil
}

func etcdGetData(txn *Transaction, key *common.Key) {
	etcdOp := clientv3.OpGet(key.String(), clientv3.WithPrefix())
	// XXX: eliminate duplicate GETs
	txn.etcd.Then = append(txn.etcd.Then, etcdOp)
}

func etcdGetByWhere(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	tableSchema, err := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if err != nil {
		return errors.New(E_INTERNAL_ERROR)
	}
	uuid, err := txn.doesWhereContainCondTypeUUID(tableSchema, txn.mapUUID, ovsOp.Where)
	if err != nil {
		return err
	}
	key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
	etcdGetData(txn, &key)
	return nil
}

func etcdCreateRow(txn *Transaction, k *common.Key, row *map[string]interface{}) error {
	key := k.String()
	val, err := makeValue(row)
	if err != nil {
		return err
	}

	etcdOp := clientv3.OpPut(key, val)
	txn.etcd.Then = append(txn.etcd.Then, etcdOp)
	return nil
}

func etcdModifyRow(txn *Transaction, k *common.Key, row *map[string]interface{}) error {
	key := k.String()
	val, err := makeValue(row)
	if err != nil {
		return err
	}

	etcdOp := clientv3.OpPut(key, val)
	txn.etcd.Then = append(txn.etcd.Then, etcdOp)
	return nil
}

func etcdDeleteRow(txn *Transaction, k *common.Key) error {
	key := k.String()
	etcdOp := clientv3.OpDelete(key)
	txn.etcd.Then = append(txn.etcd.Then, etcdOp)
	return nil
}

func (txn *Transaction) RowPrepare(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, row *map[string]interface{}) error {
	log := txn.log.WithValues("row", row)
	err := tableSchema.Unmarshal(row)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "failed to unmarshal row")
		return err
	}

	err = mapUUID.ResolvRow(txn.log, row)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "failed to resolve uuid of row")
		return err
	}

	err = tableSchema.Validate(row)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed schema validation of row")
		return err
	}
	return nil
}

/* insert */
func preInsert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	var err error
	if ovsOp.UUIDName == nil {
		return nil
	}

	if ovsOp.UUIDName != nil {
		uuid := common.GenerateUUID()
		if ovsOp.UUID != nil {
			uuid = ovsOp.UUID.GoUUID
		}
		if _, ok := txn.mapUUID[*ovsOp.UUIDName]; ok {
			err = errors.New(E_DUP_UUIDNAME)
			txn.log.Error(err, "duplicate uuid-name", "uuid-name", *ovsOp.UUIDName)
			return err
		}
		txn.mapUUID.Set(txn, *ovsOp.UUIDName, uuid)
	}

	key := common.NewTableKey(txn.request.DBName, *ovsOp.Table)
	etcdGetData(txn, &key)
	return nil
}

func doInsert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	tableSchema, err := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if err != nil {
		return errors.New(E_INTERNAL_ERROR)
	}

	uuid := common.GenerateUUID()

	if ovsOp.UUID != nil {
		uuid = ovsOp.UUID.GoUUID
	}

	if ovsOp.UUIDName != nil {
		uuid, err = txn.mapUUID.Get(txn.log, *ovsOp.UUIDName)
		if err != nil {
			txn.log.Error(err, "can't find uuid-name", "uuid-name", *ovsOp.UUIDName)
			return err
		}
	}

	for uuid := range txn.cache.Table(txn.request.DBName, *ovsOp.Table) {
		if ovsOp.UUID != nil && uuid == ovsOp.UUID.GoUUID {
			err = errors.New(E_DUP_UUID)
			txn.log.Error(err, "duplicate uuid", "uuid", *ovsOp.UUID)
			return err
		}
	}

	ovsResult.InitUUID(uuid)

	key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
	row := &map[string]interface{}{}

	*row = *ovsOp.Row
	txn.schemas.Default(txn.request.DBName, *ovsOp.Table, row)

	err = txn.RowPrepare(tableSchema, txn.mapUUID, ovsOp.Row)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed to prepare row", "row", row)
		return err
	}

	setRowUUID(row, uuid)
	setRowVersion(row)
	err = etcdCreateRow(txn, &key, row)
	*(txn.cache.Row(key)) = *row

	return err
}

/* select */
func preSelect(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doSelect(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	ovsResult.InitRows()
	tableSchema, err := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if err != nil {
		return errors.New(E_INTERNAL_ERROR)
	}

	for _, row := range txn.cache.Table(txn.request.DBName, *ovsOp.Table) {
		ok, err := txn.isRowSelectedByWhere(tableSchema, txn.mapUUID, row, ovsOp.Where)
		if err != nil {
			txn.log.Error(err, "failed to select row by where", "row", row, "where", ovsOp.Where)
			return err
		}
		if !ok {
			continue
		}
		resultRow, err := reduceRowByColumns(row, ovsOp.Columns)
		if err != nil {
			txn.log.Error(err, "failed to reduce row by columns", "row", row, "columns", ovsOp.Columns)
			return err
		}
		ovsResult.AppendRows(*resultRow)
	}
	return nil
}

/* update */
func preUpdate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doUpdate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	ovsResult.InitCount()
	tableSchema, err := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if err != nil {
		return errors.New(E_INTERNAL_ERROR)
	}
	for uuid, row := range txn.cache.Table(txn.request.DBName, *ovsOp.Table) {
		ok, err := txn.isRowSelectedByWhere(tableSchema, txn.mapUUID, row, ovsOp.Where)
		if err != nil {
			txn.log.Error(err, "failed to select row by where", "row", row, "where", ovsOp.Where)
			return err
		}
		if !ok {
			continue
		}

		err = txn.RowPrepare(tableSchema, txn.mapUUID, ovsOp.Row)
		if err != nil {
			txn.log.Error(err, "failed to prepare row", "row", ovsOp.Row)
			return err
		}

		newRow, err := txn.RowUpdate(tableSchema, txn.mapUUID, row, ovsOp.Row)
		if err != nil {
			txn.log.Error(err, "failed to update row", "row", ovsOp.Row)
			return err
		}

		setRowVersion(newRow)
		key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
		etcdModifyRow(txn, &key, newRow)
		*(txn.cache.Row(key)) = *newRow
		ovsResult.IncrementCount()
	}
	return nil
}

/* mutate */
func preMutate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doMutate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	ovsResult.InitCount()
	tableSchema, err := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if err != nil {
		return errors.New(E_INTERNAL_ERROR)
	}
	for uuid, row := range txn.cache.Table(txn.request.DBName, *ovsOp.Table) {
		ok, err := txn.isRowSelectedByWhere(tableSchema, txn.mapUUID, row, ovsOp.Where)
		if err != nil {
			txn.log.Error(err, "failed to select row by where", "row", row, "where", ovsOp.Where)
			return err
		}
		if !ok {
			continue
		}
		newRow, err := txn.RowMutate(tableSchema, txn.mapUUID, row, ovsOp.Mutations)
		if err != nil {
			txn.log.Error(err, "failed to row mutate", "row", row, "mutations", ovsOp.Mutations)
			return err
		}

		setRowVersion(newRow)
		key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
		etcdModifyRow(txn, &key, newRow)
		*(txn.cache.Row(key)) = *newRow
		ovsResult.IncrementCount()
	}
	return nil
}

/* delete */
func preDelete(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doDelete(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	ovsResult.InitCount()
	tableSchema, err := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if err != nil {
		return errors.New(E_INTERNAL_ERROR)
	}
	for uuid, row := range txn.cache.Table(txn.request.DBName, *ovsOp.Table) {
		ok, err := txn.isRowSelectedByWhere(tableSchema, txn.mapUUID, row, ovsOp.Where)
		if err != nil {
			txn.log.Error(err, "failed to select row by where", "row", row, "where", ovsOp.Where)
			return err
		}
		if !ok {
			continue
		}
		key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
		etcdDeleteRow(txn, &key)
		ovsResult.IncrementCount()
	}
	return nil
}

/* wait */
func preWait(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	var err error
	if ovsOp.Timeout == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "missing timeout parameter")
		return err
	}
	if *ovsOp.Timeout != 0 {
		txn.log.Info("ignoring non-zero wait timeout", "timeout", *ovsOp.Timeout)
	}
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

/* wait */
func doWait(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	var err error
	if ovsOp.Table == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "missing table parameter")
		return err
	}

	if ovsOp.Rows == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "missing rows parameter")
		return err
	}

	if len(*ovsOp.Rows) == 0 {
		return nil
	}

	if ovsOp.Until == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "missing until parameter")
		return err
	}

	tableSchema, errInternal := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if errInternal != nil {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed table schema lookup", "err", errInternal.Error())
		return err
	}

	var equal bool
	switch *ovsOp.Until {
	case FN_EQ:
		equal = true
	case FN_NE:
		equal = false
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "unsupported until", "until", *ovsOp.Until)
		return err
	}

	for _, actual := range txn.cache.Table(txn.request.DBName, *ovsOp.Table) {
		log := txn.log.WithValues("row", actual)
		ok, err := txn.isRowSelectedByWhere(tableSchema, txn.mapUUID, actual, ovsOp.Where)
		if err != nil {
			log.Error(err, "failed select row by where", "where", ovsOp.Where)
			return err
		}
		if !ok {
			continue
		}

		if ovsOp.Columns != nil {
			actual, err = reduceRowByColumns(actual, ovsOp.Columns)
			if err != nil {
				log.Error(err, "failed column reduction")
				return err
			}
		}

		for _, expected := range *ovsOp.Rows {
			err = txn.RowPrepare(tableSchema, txn.mapUUID, &expected)
			if err != nil {
				return err
			}

			cond, err := isEqualRow(txn, tableSchema, &expected, actual)
			log.V(5).Info("checking row equal", "expected", expected, "result", cond)
			if err != nil {
				log.Error(err, "error in row compare", "expected", expected)
				return err
			}
			if cond {
				if equal {
					return nil
				}
				err = errors.New(E_TIMEOUT)
				log.Error(err, "timed out")
				return err
			}
		}
	}

	if !equal {
		return nil
	}

	err = errors.New(E_TIMEOUT)
	txn.log.Error(err, "timed out")
	return err
}

/* commit */
func preCommit(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	var err error
	if ovsOp.Durable == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "missing durable parameter")
		return err
	}
	if *ovsOp.Durable {
		err = errors.New(E_NOT_SUPPORTED)
		txn.log.Error(err, "do not support durable == true")
		return err
	}
	return nil
}

func doCommit(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return nil
}

/* abort */
func preAbort(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return errors.New(E_ABORTED)
}

func doAbort(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return nil
}

/* comment */
func preComment(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return nil
}

func doComment(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	timestamp := time.Now().Format(time.RFC3339)
	key := common.NewCommentKey(txn.request.DBName, timestamp)
	comment := *ovsOp.Comment
	etcdOp := clientv3.OpPut(key.String(), comment)
	txn.etcd.Then = append(txn.etcd.Then, etcdOp)
	return nil
}

/* assert */
func preAssert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return nil
}

func doAssert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return nil
}
