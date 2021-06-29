package ovsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/jinzhu/copier"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

const ETCD_MAX_TXN_OPS = 128

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

func etcdOpKey(op clientv3.Op) string {
	v := reflect.ValueOf(op)
	f := v.FieldByName("key")
	k := f.Bytes()
	return string(k)
}

func (txn *Transaction) etcdRemoveDupThen() {
	newThen := []*clientv3.Op{}
	for curr, op := range txn.etcd.Then {
		etcdOpKey(op)
		//	txn.log.V(6).Info("[then] adding key", "key", key, "index", curr)
		newThen = append(newThen, &txn.etcd.Then[curr])
	}

	prevKeyIndex := map[string]int{}
	for curr, op := range newThen {
		key := etcdOpKey(*op)
		prev, ok := prevKeyIndex[key]
		if ok {
			txn.log.V(6).Info("[then] removing key", "key", key, "index", prev)
			newThen[prev] = nil
		}
		prevKeyIndex[key] = curr
	}

	txn.etcd.Then = []clientv3.Op{}
	for _, op := range newThen {
		if op != nil {
			txn.etcd.Then = append(txn.etcd.Then, *op)
		}
	}
}

func (txn *Transaction) etcdRemoveDup() {
	txn.etcdRemoveDupThen()
}

func (txn *Transaction) etcdTranaction() (*clientv3.TxnResponse, error) {
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

// XXX: move to db
type KeyValue struct {
	Key   common.Key
	Value map[string]interface{}
}

// XXX: move to db
func NewKeyValue(etcdKV *mvccpb.KeyValue) (*KeyValue, error) {
	kv := new(KeyValue)

	/* key */
	key, err := common.ParseKey(string(etcdKV.Key))
	if err != nil {
		return nil, err
	}
	kv.Key = *key
	/* value */
	err = json.Unmarshal(etcdKV.Value, &kv.Value)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

func (kv *KeyValue) Dump() {
	fmt.Printf("%s --> %v\n", kv.Key, kv.Value)
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

func (cache *Cache) RefColumnSet(txn *Transaction, columnSchema *libovsdb.ColumnSchema, value interface{}) error {
	valueSet, ok := value.(libovsdb.OvsSet)
	if !ok {
		panic(fmt.Sprintf("failed to convert value to set: %+v", value))
	}
	refTable := columnSchema.RefTable()
	refTableCache := cache.Table(txn.request.DBName, *refTable)

	refType := columnSchema.RefType()
	if refType != nil && *refType == libovsdb.Strong {
		for _, val := range valueSet.GoSet {
			uuid := val.(libovsdb.UUID)
			_, ok := refTableCache[uuid.GoUUID]
			if !ok {
				err := errors.New(E_CONSTRAINT_VIOLATION)
				txn.log.Error(err, "did not find uuid in table", "uuid", uuid, "table", *refTable)
				return err
			}
		}
	}

	return nil
}

func (cache *Cache) RefColumnMap(txn *Transaction, columnSchema *libovsdb.ColumnSchema, value interface{}) error {
	valueMap, ok := value.(libovsdb.OvsMap)
	if !ok {
		panic(fmt.Sprintf("failed to convert value to set: %+v", value))
	}
	refTable := columnSchema.RefTable()
	refTableCache := cache.Table(txn.request.DBName, *refTable)

	refType := columnSchema.RefType()
	if refType != nil && *refType == libovsdb.Strong {
		for _, val := range valueMap.GoMap {
			uuid := val.(libovsdb.UUID)
			_, ok := refTableCache[uuid.GoUUID]
			if !ok {
				err := errors.New(E_CONSTRAINT_VIOLATION)
				txn.log.Error(err, "did not find uuid in table", "uuid", uuid, "table", *refTable)
				return err
			}
		}
	}

	return nil
}

func (cache *Cache) RefColumn(txn *Transaction, columnSchema *libovsdb.ColumnSchema, value interface{}) error {
	var err error
	refTable := columnSchema.RefTable()
	if refTable == nil {
		return nil
	}

	switch columnSchema.Type {
	case libovsdb.TypeSet:
		err = cache.RefColumnSet(txn, columnSchema, value)
	case libovsdb.TypeMap:
		err = cache.RefColumnMap(txn, columnSchema, value)
	}
	if err != nil {
		txn.log.Error(err, "failed schema reftype")
		return err
	}

	return nil
}

func (cache *Cache) RefTable(txn *Transaction, tableSchema *libovsdb.TableSchema, tableCache *TableCache) error {
	for _, row := range *tableCache {
		for column, value := range *row {
			columnSchema, err := tableSchema.LookupColumn(column)
			if err != nil {
				txn.log.Error(err, "failed schema reftype")
				return err
			}
			err = cache.RefColumn(txn, columnSchema, value)
			if err != nil {
				txn.log.Error(err, "failed schema reftype")
				return err
			}
		}
	}
	return nil
}

func (cache *Cache) Ref(txn *Transaction) error {
	for database, databaseCache := range *cache {
		for table, tableCache := range databaseCache {
			tableSchema, err := txn.schemas.LookupTable(database, table)
			if err != nil {
				txn.log.Error(err, "failed schema reftype")
				return err
			}
			err = cache.RefTable(txn, tableSchema, &tableCache)
			if err != nil {
				txn.log.Error(err, "failed schema reftype")
				return err
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

func (mapUUID MapUUID) Get(txn *Transaction, uuidName string) (string, error) {
	uuid, ok := mapUUID[uuidName]
	if !ok {
		err := errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "can't get named-uuid", "uuid-name", uuidName)
		return "", err
	}
	return uuid, nil
}

func (mapUUID MapUUID) ResolvUUID(txn *Transaction, value interface{}) (interface{}, error) {
	namedUuid, _ := value.(libovsdb.UUID)
	if namedUuid.GoUUID != "" && namedUuid.ValidateUUID() != nil {
		uuid, err := mapUUID.Get(txn, namedUuid.GoUUID)
		if err != nil {
			return nil, err
		}
		value = libovsdb.UUID{GoUUID: uuid}
	}
	return value, nil
}

func (mapUUID MapUUID) ResolvSet(txn *Transaction, value interface{}) (interface{}, error) {
	oldset, _ := value.(libovsdb.OvsSet)
	newset := libovsdb.OvsSet{}
	for _, oldval := range oldset.GoSet {
		newval, err := mapUUID.ResolvUUID(txn, oldval)
		if err != nil {
			return nil, err
		}
		newset.GoSet = append(newset.GoSet, newval)
	}
	return newset, nil
}

func (mapUUID MapUUID) ResolvMap(txn *Transaction, value interface{}) (interface{}, error) {
	oldmap, _ := value.(libovsdb.OvsMap)
	newmap := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}
	for key, oldval := range oldmap.GoMap {
		newval, err := mapUUID.ResolvUUID(txn, oldval)
		if err != nil {
			return nil, err
		}
		newmap.GoMap[key] = newval
	}
	return newmap, nil
}

func (mapUUID MapUUID) Resolv(txn *Transaction, value interface{}) (interface{}, error) {
	switch value.(type) {
	case libovsdb.UUID:
		return mapUUID.ResolvUUID(txn, value)
	case libovsdb.OvsSet:
		return mapUUID.ResolvSet(txn, value)
	case libovsdb.OvsMap:
		return mapUUID.ResolvMap(txn, value)
	default:
		return value, nil
	}
}

func (mapUUID MapUUID) ResolvRow(txn *Transaction, row *map[string]interface{}) error {
	for column, value := range *row {
		value, err := mapUUID.Resolv(txn, value)
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

type EventKeyValue struct {
	Key            string `json:"key"`
	CreateRevision int64  `json:"create_revision"`
	ModRevision    int64  `json:"mod_revision"`
	Version        int64  `json:"version"`
	Value          string `json:"value"`
	Lease          int64  `json:"lease"`
}

func NewEventKeyValue(kv *mvccpb.KeyValue) *EventKeyValue {
	if kv == nil {
		return nil
	}
	return &EventKeyValue{
		Key:            string(kv.Key),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Value:          string(kv.Value),
		Lease:          kv.Lease,
	}
}

type Event struct {
	Type   string         `json:"type"`
	Kv     *EventKeyValue `json:"kv"`
	PrevKv *EventKeyValue `json:"prev_kv"`
}

func NewEvent(ev *clientv3.Event) Event {
	return Event{
		Type:   string(ev.Type),
		Kv:     NewEventKeyValue(ev.Kv),
		PrevKv: NewEventKeyValue(ev.PrevKv),
	}
}

type EventList []Event

func NewEventList(events []*clientv3.Event) EventList {
	printable := EventList{}
	for _, ev := range events {
		if ev != nil {
			printable = append(printable, NewEvent(ev))
		}
	}
	return printable
}

func (evList EventList) String() string {
	b, _ := json.Marshal(evList)
	return string(b)
}

func NewEtcd(parent *Etcd) *Etcd {
	return &Etcd{
		Ctx: parent.Ctx,
		Cli: parent.Cli,
	}
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

type TxnLock struct {
	root      sync.Mutex
	databases map[string]*sync.Mutex
}

type Transaction struct {
	/* logger */
	log logr.Logger

	/* lock */
	lock *TxnLock

	/* ovs */
	schemas  libovsdb.Schemas
	request  libovsdb.Transact
	response libovsdb.TransactResponse

	/* cache */
	cache   Cache
	mapUUID MapUUID

	/* etcd */
	etcd *Etcd
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

	/* verify that select is not intermixed with other operations */
	hasSelect := false
	hasOther := false
	for _, ovsOp := range txn.request.Operations {
		if ovsOp.Op == OP_SELECT {
			hasSelect = true
		} else {
			hasOther = true
		}
	}
	if hasSelect && hasOther {
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

		if err = txn.cache.Validate(txn, txn.schemas); err != nil {
			panic(fmt.Sprintf("validation of %s failed: %s", ovsOp, err.Error()))
		}
	}
	_, err = txn.etcdTranaction()
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

	err = txn.cache.Ref(txn)
	if err != nil {
		errStr := err.Error()
		txn.response.Error = &errStr
		return -1, err
	}

	txn.etcdRemoveDup()
	trResponse, err := txn.etcdTranaction()
	if err != nil {
		errStr := err.Error()
		txn.response.Error = &errStr
		return -1, err
	}

	txn.log.V(5).Info("commit transaction", "response", txn.response)
	return trResponse.Header.Revision, nil
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

const (
	FN_LT = "<"
	FN_LE = "<="
	FN_EQ = "=="
	FN_NE = "!="
	FN_GE = ">="
	FN_GT = ">"
	FN_IN = "includes"
	FN_EX = "excludes"
)

type Condition struct {
	Column       string
	Function     string
	Value        interface{}
	ColumnSchema *libovsdb.ColumnSchema
	txn          *Transaction
}

func NewCondition(txn *Transaction, tableSchema *libovsdb.TableSchema, mapUUID MapUUID, condition []interface{}) (*Condition, error) {
	var err error
	if len(condition) != 3 {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "expected 3 elements in condition", "condition", condition)
		return nil, err
	}

	column, ok := condition[0].(string)
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed to convert column to string", "condition", condition)
		return nil, err
	}

	columnSchema, err := tableSchema.LookupColumn(column)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed schema lookup", "column", column)
		return nil, err
	}

	fn, ok := condition[1].(string)
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed to convert function to string", "condition", condition)
		return nil, err
	}

	value := condition[2]
	if columnSchema != nil {
		tmp, err := columnSchema.Unmarshal(value)
		if err != nil {
			err = errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "failed to unmarsahl condition", "column", column, "type", columnSchema.Type, "value", value)
			return nil, err
		}
		value = tmp
	} else if column == libovsdb.COL_UUID {
		tmp, err := libovsdb.UnmarshalUUID(value)
		if err != nil {
			err = errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "failed to unamrshal condition", "column", column, "type", libovsdb.TypeUUID, "value", value)
			return nil, err
		}
		value = tmp
	}

	tmp, err := mapUUID.Resolv(txn, value)
	if err != nil {
		err := errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed to resolve named-uuid condition", "column", column, "value", value)
		return nil, err
	}
	value = tmp

	return &Condition{
		Column:       column,
		Function:     fn,
		Value:        value,
		ColumnSchema: columnSchema,
		txn:          txn,
	}, nil
}

func (c *Condition) CompareInteger(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}
	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	if fn == FN_GT && actual > expected {
		return true, nil
	}
	if fn == FN_GE && actual >= expected {
		return true, nil
	}
	if fn == FN_LT && actual < expected {
		return true, nil
	}
	if fn == FN_LE && actual <= expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareReal(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	if fn == FN_GT && actual > expected {
		return true, nil
	}
	if fn == FN_GE && actual >= expected {
		return true, nil
	}
	if fn == FN_LT && actual < expected {
		return true, nil
	}
	if fn == FN_LE && actual <= expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareBoolean(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(bool)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(bool)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareString(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareUUID(row *map[string]interface{}) (bool, error) {
	var err error
	var actual libovsdb.UUID
	ar, ok := (*row)[c.Column].([]interface{})
	if ok {
		actual = libovsdb.UUID{GoUUID: ar[1].(string)}
	} else {
		actual, ok = (*row)[c.Column].(libovsdb.UUID)
		if !ok {
			err = errors.New(E_CONSTRAINT_VIOLATION)
			c.txn.log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
			return false, err
		}
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.UUID)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual.GoUUID == expected.GoUUID {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual.GoUUID != expected.GoUUID {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareEnum(row *map[string]interface{}) (bool, error) {
	var err error
	switch c.ColumnSchema.TypeObj.Key.Type {
	case libovsdb.TypeString:
		return c.CompareString(row)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "does not support type as enum key", "type", c.ColumnSchema.TypeObj.Key.Type)
		return false, err
	}
}

func (c *Condition) CompareSet(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && isEqualSet(actual, expected) {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && !isEqualSet(actual, expected) {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareMap(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(libovsdb.OvsMap)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.OvsMap)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && isEqualMap(actual, expected) {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && !isEqualMap(actual, expected) {
		return true, nil
	}
	return false, nil
}

func (c *Condition) Compare(row *map[string]interface{}) (bool, error) {
	switch c.ColumnSchema.Type {
	case libovsdb.TypeInteger:
		return c.CompareInteger(row)
	case libovsdb.TypeReal:
		return c.CompareReal(row)
	case libovsdb.TypeBoolean:
		return c.CompareBoolean(row)
	case libovsdb.TypeString:
		return c.CompareString(row)
	case libovsdb.TypeUUID:
		return c.CompareUUID(row)
	case libovsdb.TypeEnum:
		return c.CompareEnum(row)
	case libovsdb.TypeSet:
		return c.CompareSet(row)
	case libovsdb.TypeMap:
		return c.CompareMap(row)
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		c.txn.log.Error(err, "usupported type comparison", "type", c.ColumnSchema.Type)
		return false, err
	}
}

func (txn *Transaction) getUUIDIfExists(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, cond1 interface{}) (string, error) {
	var err error
	cond2, ok := cond1.([]interface{})
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed to convert condition", "condition", cond1)
		return "", err
	}
	condition, err := NewCondition(txn, tableSchema, mapUUID, cond2)
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
	condition, err := NewCondition(txn, tableSchema, mapUUID, cond)
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

const (
	MT_SUM        = "+="
	MT_DIFFERENCE = "-="
	MT_PRODUCT    = "*="
	MT_QUOTIENT   = "/="
	MT_REMAINDER  = "%="
	MT_INSERT     = "insert"
	MT_DELETE     = "delete"
)

type Mutation struct {
	Column       string
	Mutator      string
	Value        interface{}
	ColumnSchema *libovsdb.ColumnSchema
	txn          *Transaction
}

func NewMutation(txn *Transaction, tableSchema *libovsdb.TableSchema, mapUUID MapUUID, mutation []interface{}) (*Mutation, error) {
	var err error
	if len(mutation) != 3 {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "expected 3 items in mutation object", "mutation", mutation)
		return nil, err
	}

	column, ok := mutation[0].(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "can't convert mutation column", mutation[0])
		return nil, err
	}

	columnSchema, err := tableSchema.LookupColumn(column)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "can't find column schema", "column", column)
		return nil, err
	}

	mt, ok := mutation[1].(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "can't convert mutation mutator", "mutator", mutation[1])
		return nil, err
	}

	value := mutation[2]

	value, err = columnSchema.Unmarshal(value)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed unmarshal of column", "column", column)
		return nil, err
	}

	value, err = mapUUID.Resolv(txn, value)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed resolv-namedUUID of column", "column", column)
		return nil, err
	}

	err = columnSchema.Validate(value)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed validate of column", "column", column)
		return nil, err
	}

	return &Mutation{
		Column:       column,
		Mutator:      mt,
		Value:        value,
		ColumnSchema: columnSchema,
		txn:          txn,
	}, nil
}

func (m *Mutation) MutateInteger(row *map[string]interface{}) error {
	var err error
	original := (*row)[m.Column].(int)
	value, ok := m.Value.(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "can't convert mutation value", "value", m.Value)
		return err
	}
	mutated := original
	switch m.Mutator {
	case MT_SUM:
		mutated += value
	case MT_DIFFERENCE:
		mutated -= value
	case MT_PRODUCT:
		mutated *= value
	case MT_QUOTIENT:
		if value != 0 {
			mutated /= value
		} else {
			err = errors.New(E_DOMAIN_ERROR)
			m.txn.log.Error(err, "can't devide by 0")
			return err
		}
	case MT_REMAINDER:
		if value != 0 {
			mutated %= value
		} else {
			err = errors.New(E_DOMAIN_ERROR)
			m.txn.log.Error(err, "can't modulo by 0")
			return err
		}
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "unsupported mutator", "mutator", m.Mutator)
		return err
	}
	(*row)[m.Column] = mutated
	return nil
}

func (m *Mutation) MutateReal(row *map[string]interface{}) error {
	var err error
	original := (*row)[m.Column].(float64)
	value, ok := m.Value.(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "failed to convert mutation value", "value", m.Value)
		return err
	}
	mutated := original
	switch m.Mutator {
	case MT_SUM:
		mutated += value
	case MT_DIFFERENCE:
		mutated -= value
	case MT_PRODUCT:
		mutated *= value
	case MT_QUOTIENT:
		if value != 0 {
			mutated /= value
		} else {
			err = errors.New(E_DOMAIN_ERROR)
			m.txn.log.Error(err, "can't devide by 0")
			return err
		}
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "unsupported mutator", "mutator", m.Mutator)
		return err
	}
	(*row)[m.Column] = mutated
	return nil
}

func inSet(set *libovsdb.OvsSet, a interface{}) bool {
	for _, b := range set.GoSet {
		if isEqualValue(a, b) {
			return true
		}
	}
	return false
}

func (m *Mutation) insertToSet(original *libovsdb.OvsSet, toInsert interface{}) (*libovsdb.OvsSet, error) {
	var err error
	toInsertSet, ok := toInsert.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "failed to convert mutation value", "value", toInsert)
		return nil, err
	}
	mutated := new(libovsdb.OvsSet)
	copier.Copy(mutated, original)
	for _, v := range toInsertSet.GoSet {
		if !inSet(original, v) {
			mutated.GoSet = append(mutated.GoSet, v)
		}
	}
	return mutated, nil
}

func (m *Mutation) deleteFromSet(original *libovsdb.OvsSet, toDelete interface{}) (*libovsdb.OvsSet, error) {
	var err error
	toDeleteSet, ok := toDelete.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "failed to convert mutation value", "value", toDelete)
		return nil, err
	}
	mutated := new(libovsdb.OvsSet)
	for _, current := range original.GoSet {
		found := false
		for _, v := range toDeleteSet.GoSet {
			if isEqualValue(current, v) {
				found = true
				break
			}
		}
		if !found {
			mutated.GoSet = append(mutated.GoSet, current)
		}
	}
	return mutated, nil
}

func (m *Mutation) MutateSet(row *map[string]interface{}) error {
	original := (*row)[m.Column].(libovsdb.OvsSet)
	var mutated *libovsdb.OvsSet
	var err error
	switch m.Mutator {
	case MT_INSERT:
		mutated, err = m.insertToSet(&original, m.Value)
	case MT_DELETE:
		mutated, err = m.deleteFromSet(&original, m.Value)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "unsupported mutation mutator:", "mutator", m.Mutator)
	}
	if err != nil {
		return err
	}
	(*row)[m.Column] = *mutated
	return nil
}

func (m *Mutation) insertToMap(original *libovsdb.OvsMap, toInsert interface{}) (*libovsdb.OvsMap, error) {
	mutated := new(libovsdb.OvsMap)
	copier.Copy(&mutated, &original)
	switch toInsert := toInsert.(type) {
	case libovsdb.OvsMap:
		for k, v := range toInsert.GoMap {
			if _, ok := mutated.GoMap[k]; !ok {
				mutated.GoMap[k] = v
			}
		}
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "unsupported mutator value type", "value", toInsert)
		return nil, err
	}
	return mutated, nil
}

func (m *Mutation) deleteFromMap(original *libovsdb.OvsMap, toDelete interface{}) (*libovsdb.OvsMap, error) {
	mutated := new(libovsdb.OvsMap)
	copier.Copy(&mutated, &original)
	switch toDelete := toDelete.(type) {
	case libovsdb.OvsMap:
		for k, v := range toDelete.GoMap {
			if mutated.GoMap[k] == v {
				delete(mutated.GoMap, k)
			}
		}
	case libovsdb.OvsSet:
		for _, k := range toDelete.GoSet {
			delete(mutated.GoMap, k)
		}
	}
	return mutated, nil
}

func (m *Mutation) MutateMap(row *map[string]interface{}) error {
	original := (*row)[m.Column].(libovsdb.OvsMap)
	mutated := new(libovsdb.OvsMap)
	var err error
	switch m.Mutator {
	case MT_INSERT:
		mutated, err = m.insertToMap(&original, m.Value)
	case MT_DELETE:
		mutated, err = m.deleteFromMap(&original, m.Value)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "unsupported mutation mutator", "mutator", m.Mutator)
		return err
	}
	if err != nil {
		return err
	}
	(*row)[m.Column] = *mutated
	return nil
}

func DefaultColumn(tableSchema *libovsdb.TableSchema, row *map[string]interface{}, column string) error {
	columnSchema, err := tableSchema.LookupColumn(column)
	if err != nil {
		return err
	}
	if (*row)[column] == nil {
		(*row)[column] = columnSchema.Default()
	}
	return nil
}

func (m *Mutation) Mutate(tableSchema *libovsdb.TableSchema, row *map[string]interface{}) error {
	var err error

	switch m.Column {
	case libovsdb.COL_UUID, libovsdb.COL_VERSION:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "can't mutate column", "column", m.Column)
		return err
	}
	if m.ColumnSchema.Mutable != nil && !*m.ColumnSchema.Mutable {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "can't mutate unmutable column", "column", m.Column)
		return err
	}
	err = DefaultColumn(tableSchema, row, m.Column)
	if err != nil {
		xErr := errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "failed update of non initialized column", "column", m.Column)
		return xErr
	}

	switch m.ColumnSchema.Type {
	case libovsdb.TypeInteger:
		return m.MutateInteger(row)
	case libovsdb.TypeReal:
		return m.MutateReal(row)
	case libovsdb.TypeSet:
		return m.MutateSet(row)
	case libovsdb.TypeMap:
		return m.MutateMap(row)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.txn.log.Error(err, "unsupported column schema type", "type", m.ColumnSchema.Type)
		return err
	}
}

func (txn *Transaction) RowMutate(tableSchema *libovsdb.TableSchema, mapUUID MapUUID, oldRow *map[string]interface{}, mutations *[]interface{}) (*map[string]interface{}, error) {
	newRow := &map[string]interface{}{}
	copier.Copy(newRow, oldRow)
	err := tableSchema.Unmarshal(newRow)
	if err != nil {
		return nil, err
	}
	for _, mt := range *mutations {
		m, err := NewMutation(txn, tableSchema, mapUUID, mt.([]interface{}))
		if err != nil {
			return nil, err
		}

		err = m.Mutate(tableSchema, newRow)
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
		err = DefaultColumn(tableSchema, newRow, column)
		if err != nil {
			xErr := errors.New(E_CONSTRAINT_VIOLATION)
			txn.log.Error(err, "failed update of non initialized column", "column", column)
			return nil, xErr
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

func etcdGetByRef(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	tableSchema, err := txn.schemas.LookupTable(txn.request.DBName, *ovsOp.Table)
	if err != nil {
		txn.log.Error(err, "failed etcd get by ref")
		return errors.New(E_INTERNAL_ERROR)
	}
	tables := tableSchema.RefTables(txn.request.DBName, *ovsOp.Table)
	for _, table := range tables {
		key := common.NewTableKey(txn.request.DBName, table)
		etcdGetData(txn, &key)
	}
	return nil
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

func etcdEventIsCreate(ev *clientv3.Event) bool {
	if ev.Type != mvccpb.PUT {
		return false
	}
	return ev.Kv.CreateRevision == ev.Kv.ModRevision
}

func etcdEventIsModify(ev *clientv3.Event) bool {
	if ev.Type != mvccpb.PUT {
		return false
	}
	return ev.Kv.CreateRevision < ev.Kv.ModRevision
}

func etcdEventCreateFromModify(ev *clientv3.Event) *clientv3.Event {
	key := string(ev.Kv.Key)
	val := string(ev.Kv.Value)
	return etcdEventCreate(key, val)
}

func etcdEventCreate(key, val string) *clientv3.Event {
	return &clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key:            []byte(key),
			Value:          []byte(val),
			CreateRevision: 1,
			ModRevision:    1,
		},
	}

}

func etcdEventModify(key, val, prevVal string) *clientv3.Event {
	return &clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key:            []byte(key),
			Value:          []byte(val),
			CreateRevision: 1,
			ModRevision:    2,
		},
		PrevKv: &mvccpb.KeyValue{
			Key:            []byte(key),
			Value:          []byte(prevVal),
			CreateRevision: 1,
			ModRevision:    1,
		},
	}
}

func etcdEventDelete(key, prevVal string) *clientv3.Event {
	return &clientv3.Event{
		Type: mvccpb.DELETE,
		PrevKv: &mvccpb.KeyValue{
			Key:            []byte(key),
			Value:          []byte(prevVal),
			CreateRevision: 1,
			ModRevision:    1,
		},
	}
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

	err = mapUUID.ResolvRow(txn, row)
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
	err := etcdGetByRef(txn, ovsOp, ovsResult)
	if err != nil {
		return err
	}

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
		uuid, err = txn.mapUUID.Get(txn, *ovsOp.UUIDName)
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
	err := etcdGetByRef(txn, ovsOp, ovsResult)
	if err != nil {
		return err
	}
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
	err := etcdGetByRef(txn, ovsOp, ovsResult)
	if err != nil {
		return err
	}
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
	err := etcdGetByRef(txn, ovsOp, ovsResult)
	if err != nil {
		return err
	}
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
	key := common.NewCommentKey(timestamp)
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
