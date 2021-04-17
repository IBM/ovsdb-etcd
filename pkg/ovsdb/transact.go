package ovsdb

// TODO: named-uuid from one operation servces other operation
// TODO: check that etcd revision of data GOT did not change when PUT
// TODO: add re-try mechanism for transactions
// TODO: add schema to initiate default values
// TODO: add schema to trigger correct type based mutate
// TODO: add schema and verify no attempt to mutate/update a read-only column
// TODO: every operation must be recorded in the journal db/txn/operation (see
// implementation)
// TODO: add schema to validate constrains
// TODO: add schema to validate uniqness "key" columns
// TODO: add schema to support references (references constrains)
// TODO: add schema to support GC
// TODO: Support the "Cancel" method
// TODO: log errors to klog
// TODO: https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/#insert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	guuid "github.com/google/uuid"
	"github.com/jinzhu/copier"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

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
	E_INTERNAL_ERROR   = "internal error"
	E_OVSDB_ERROR      = "ovsdb error"
	E_PERMISSION_ERROR = "permission error"
	E_SYNTAX_ERROR     = "syntax error or unknown column"
)

// XXX: move libovsdb
func isEqualRows(a, b []map[string]interface{}) bool {
	return reflect.DeepEqual(a, b)
}

// XXX: move libovsdb
func isEqualSet(a, b libovsdb.OvsSet) bool {
	return reflect.DeepEqual(a.GoSet, b.GoSet) // XXX: should I sort first?
}

// XXX: move libovsdb
func isEqualMap(a, b libovsdb.OvsMap) bool {
	return reflect.DeepEqual(a.GoMap, b.GoMap)
}

// XXX: move libovsdb
func isEqual(a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}

// XXX: move to db
const DELIM = "/"

// XXX: move to libovsdb
const (
	COL_UUID     = "_uuid"
	COL_VERSION  = "_version"
	COL_UUIDNAME = "uuid-name"
)

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

// XXX: move to db
func NewEtcdClient(endpoints []string) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (txn *Transaction) etcdTranaction() (*clientv3.TxnResponse, error) {
	res, err := txn.etcdCli.Txn(txn.etcdCtx).If(txn.etcdIf...).Then(txn.etcdThen...).Else(txn.etcdElse...).Commit()

	// XXX: reduce identical get calls

	// remove previois put operations
	for _, r := range res.Responses {
		switch v := r.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			txn.data.PopulateFromKV(v.ResponseRange.Kvs)
		}
	}

	txn.etcdIf = []clientv3.Cmp{}
	txn.etcdThen = []clientv3.Op{}
	txn.etcdElse = []clientv3.Op{}

	return res, err
}

// XXX: move to db
type KeyValue struct {
	DBName string
	Table  string
	UUID   string
	Value  map[string]interface{}
}

// XXX: move to db
func NewKeyValue(etcdKV *mvccpb.KeyValue) (*KeyValue, error) {
	kv := new(KeyValue)

	/* key */
	split := strings.Split(string(etcdKV.Key), DELIM)
	if len(split) != 3 {
		return nil, errors.New(E_INTERNAL_ERROR)
	}
	kv.DBName = split[0]
	kv.Table = split[1]
	kv.UUID = split[2]

	/* value */
	err := json.Unmarshal(etcdKV.Value, &kv.Value)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

func (kv *KeyValue) Dump() {
	fmt.Printf("%s/%s/%s --> %v\n", kv.DBName, kv.Table, kv.UUID, kv.Value)
}

type Database map[string]Table
type Table map[string]*map[string]interface{}

type Cache struct {
	db Database
}

func (c *Cache) Table(table string) Table {
	if c.db == nil {
		c.db = Database{}
	}
	t, ok := c.db[table]
	if !ok {
		t = Table{}
		c.db[table] = t
	}
	return t
}

func (c *Cache) Row(table, uuid string) *map[string]interface{} {
	t := c.Table(table)
	_, ok := t[uuid]
	if !ok {
		t[uuid] = new(map[string]interface{})
	}
	return t[uuid]
}

func (c *Cache) PopulateFromKV(kvs []*mvccpb.KeyValue) error {
	for _, x := range kvs {
		kv, err := NewKeyValue(x)
		if err != nil {
			return err
		}
		row := c.Row(kv.Table, kv.UUID)
		(*row) = kv.Value
	}
	return nil
}

type Transaction struct {
	/* ovs */
	request  libovsdb.Transact
	response libovsdb.TransactResponse

	/* cache */
	data Cache

	/* etcd */
	etcdCli  *clientv3.Client
	etcdCtx  context.Context
	etcdIf   []clientv3.Cmp
	etcdThen []clientv3.Op
	etcdElse []clientv3.Op
}

func NewTransaction(cli *clientv3.Client, request *libovsdb.Transact) *Transaction {
	txn := new(Transaction)
	txn.request = *request
	txn.response.Result = make([]libovsdb.OperationResult, len(request.Operations))
	txn.etcdCtx = context.TODO()
	txn.etcdCli = cli
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

func (txn *Transaction) Commit() error {

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
		txn.response.Error = err.Error()
		return err
	}

	/* fetch needed data from database needed to perform the operation */
	for i, ovsOp := range txn.request.Operations {
		if err = ovsOpCallbackMap[ovsOp.Op][0](txn, &ovsOp, &txn.response.Result[i]); err != nil {
			txn.response.Error = err.Error()
			return err
		}
	}
	_, err = txn.etcdTranaction()
	if err != nil {
		txn.response.Error = err.Error()
		return err
	}

	/* commit actual transactional changes to database */
	for i, ovsOp := range txn.request.Operations {
		err = ovsOpCallbackMap[ovsOp.Op][1](txn, &ovsOp, &txn.response.Result[i])
		if err != nil {
			txn.response.Error = err.Error()
			return err
		}
	}
	_, err = txn.etcdTranaction()
	if err != nil {
		txn.response.Error = err.Error()
		return err
	}

	return nil
}

// XXX: move to db
func makePrefix(dbname, table string) string {
	return dbname + DELIM + table
}

// XXX: move to db
// XXX: ovsdb/<server>/<dbname>/tables/<table>/<uuid>
// XXX: ovsdb/<server>/<dbname>/locks/<id>
// XXX: ovsdb/<server>/<dbname>/comments/<timestmap>
func makeKey(dbname, table, uuid string) string {
	return dbname + DELIM + table + DELIM + uuid
}

// XXX: move to db
func makeValue(row *map[string]interface{}) (string, error) {
	b, err := json.Marshal(*row)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// TODO: we should not add uuid to etcd
func setRowUUID(row *map[string]interface{}, uuid string) {
	(*row)[COL_UUID] = uuid
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
	Column   string
	Function string
	Value    interface{}
}

func NewCondition(condition []interface{}) (*Condition, error) {
	if len(condition) != 3 {
		return nil, errors.New(E_INTERNAL_ERROR)
	}

	column, ok := condition[0].(string)
	if !ok {
		return nil, errors.New(E_INTERNAL_ERROR)
	}

	fn, ok := condition[1].(string)
	if !ok {
		return nil, errors.New(E_INTERNAL_ERROR)
	}

	value := condition[2]
	return &Condition{Column: column, Function: fn, Value: value}, nil
}

func (c *Condition) CompareInteger(row *map[string]interface{}) (bool, error) {
	actual, ok := (*row)[c.Column].(int)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
	fn := c.Function
	expected, ok := c.Value.(int)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
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
	actual, ok := (*row)[c.Column].(float64)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
	fn := c.Function
	expected, ok := c.Value.(float64)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
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
	actual, ok := (*row)[c.Column].(bool)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
	fn := c.Function
	expected, ok := c.Value.(bool)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
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
	actual, ok := (*row)[c.Column].(string)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
	fn := c.Function
	expected, ok := c.Value.(string)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
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
	actual, ok := (*row)[c.Column].(libovsdb.UUID)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.UUID)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}

	if (fn == FN_EQ || fn == FN_IN) && actual.GoUUID == expected.GoUUID {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual.GoUUID != expected.GoUUID {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareSet(row *map[string]interface{}) (bool, error) {
	actual, ok := (*row)[c.Column].(libovsdb.OvsSet)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.OvsSet)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
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
	actual, ok := (*row)[c.Column].(libovsdb.OvsMap)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.OvsMap)
	if !ok {
		return false, errors.New(E_CONSTRAINT_VIOLATION)
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
	switch (*row)[c.Column].(type) {
	case int:
		return c.CompareInteger(row)
	case float64:
		return c.CompareReal(row)
	case bool:
		return c.CompareBoolean(row)
	case string:
		return c.CompareString(row)
	case libovsdb.OvsSet:
		return c.CompareSet(row)
	case libovsdb.OvsMap:
		return c.CompareMap(row)
	case libovsdb.UUID:
		return c.CompareUUID(row)
	default:
		return false, errors.New(E_CONSTRAINT_VIOLATION)
	}
}

func getUUIDIfExists(cond1 interface{}) (string, error) {
	cond2, ok := cond1.([]interface{})
	if !ok {
		return "", errors.New(E_INTERNAL_ERROR)
	}
	condition, err := NewCondition(cond2)
	if err != nil {
		return "", err
	}
	if condition.Column != COL_UUID {
		return "", nil
	}
	if condition.Function != FN_EQ && condition.Function != FN_IN {
		return "", nil
	}
	ovsUUID, ok := condition.Value.(libovsdb.UUID)
	if !ok {
		return "", errors.New(E_INTERNAL_ERROR)
	}
	// XXX: replace libovsdb.validateUUID() implementation
	_, err = guuid.Parse(ovsUUID.GoUUID)
	if err != nil {
		return "", err
	}
	return ovsUUID.GoUUID, err
}

func doesWhereContainCondTypeUUID(where []interface{}) (string, error) {
	for _, c := range where {
		cond, ok := c.([]interface{})
		if !ok {
			return "", errors.New(E_INTERNAL_ERROR)
		}
		uuid, err := getUUIDIfExists(cond)
		if err != nil {
			return "", err
		}
		if uuid != "" {
			return uuid, nil
		}
	}
	return "", nil

}

func isRowSelectedByWhere(row *map[string]interface{}, where []interface{}) (bool, error) {
	for _, c := range where {
		cond, ok := c.([]interface{})
		if !ok {
			return false, errors.New(E_INTERNAL_ERROR)
		}
		ok, err := isRowSelectedByCond(row, cond)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func isRowSelectedByCond(row *map[string]interface{}, cond []interface{}) (bool, error) {
	condition, err := NewCondition(cond)
	if err != nil {
		return false, err
	}
	return condition.Compare(row)
}

// XXX: shared with monitors
func reduceRowByColumns(row *map[string]interface{}, columns []string) *map[string]interface{} {
	return row // FIXME: missing implementation
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
	Column  string
	Mutator string
	Value   interface{}
}

func NewMutation(mutation []interface{}) (*Mutation, error) {
	if len(mutation) != 3 {
		return nil, errors.New(E_CONSTRAINT_VIOLATION)
	}

	column, ok := mutation[0].(string)
	if !ok {
		return nil, errors.New(E_CONSTRAINT_VIOLATION)
	}

	mt, ok := mutation[1].(string)
	if !ok {
		return nil, errors.New(E_CONSTRAINT_VIOLATION)
	}

	value := mutation[2]
	return &Mutation{Column: column, Mutator: mt, Value: value}, nil
}

func (m *Mutation) MutateInteger(row *map[string]interface{}) error {
	original := (*row)[m.Column].(int)
	value := m.Value.(int)
	mutated := original
	var err error
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
		}
	case MT_REMAINDER:
		if value != 0 {
			mutated %= value
		} else {
			err = errors.New(E_DOMAIN_ERROR)
		}
	default:
		return errors.New(E_CONSTRAINT_VIOLATION)
	}
	(*row)[m.Column] = mutated
	return err
}

func (m *Mutation) MutateReal(row *map[string]interface{}) error {
	original := (*row)[m.Column].(float64)
	value := m.Value.(float64)
	mutated := original
	var err error
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
		}
	default:
		return errors.New(E_CONSTRAINT_VIOLATION)
	}
	(*row)[m.Column] = mutated
	return err
}

func insertToSet(original *libovsdb.OvsSet, toInsert interface{}) (*libovsdb.OvsSet, error) {
	toInsertSet, ok := toInsert.(libovsdb.OvsSet)
	if !ok {
		return nil, errors.New(E_CONSTRAINT_VIOLATION)
	}
	mutated := new(libovsdb.OvsSet)
	copier.Copy(mutated, original)
	for _, v := range toInsertSet.GoSet {
		mutated.GoSet = append(mutated.GoSet, v)
	}
	return mutated, nil
}

func deleteFromSet(original *libovsdb.OvsSet, toDelete interface{}) (*libovsdb.OvsSet, error) {
	toDeleteSet, ok := toDelete.(libovsdb.OvsSet)
	if !ok {
		return nil, errors.New(E_CONSTRAINT_VIOLATION)
	}
	mutated := new(libovsdb.OvsSet)
	for _, current := range original.GoSet {
		found := false
		for _, v := range toDeleteSet.GoSet {
			if isEqual(current, v) {
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
		mutated, err = insertToSet(&original, m.Value)
	case MT_DELETE:
		mutated, err = deleteFromSet(&original, m.Value)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
	}
	if err != nil {
		return err
	}
	(*row)[m.Column] = *mutated
	return nil
}

func insertToMap(original *libovsdb.OvsMap, toInsert interface{}) (*libovsdb.OvsMap, error) {
	mutated := new(libovsdb.OvsMap)
	copier.Copy(&mutated, &original)
	switch toInsert := toInsert.(type) {
	case libovsdb.OvsMap:
		for k, v := range toInsert.GoMap {
			mutated.GoMap[k] = v
		}
	default:
		return nil, errors.New(E_CONSTRAINT_VIOLATION)
	}
	return mutated, nil
}

func deleteFromMap(original *libovsdb.OvsMap, toDelete interface{}) (*libovsdb.OvsMap, error) {
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
		mutated, err = insertToMap(&original, m.Value)
	case MT_DELETE:
		mutated, err = deleteFromMap(&original, m.Value)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
	}
	if err != nil {
		return err
	}
	(*row)[m.Column] = mutated
	return nil
}

func (m *Mutation) Mutate(row *map[string]interface{}) error {
	switch m.Column {
	case COL_UUID, COL_VERSION:
		return errors.New(E_CONSTRAINT_VIOLATION)
	}
	switch (*row)[m.Column].(type) {
	case int:
		return m.MutateInteger(row)
	case float64:
		return m.MutateReal(row)
	case libovsdb.OvsSet:
		return m.MutateSet(row)
	case libovsdb.OvsMap:
		return m.MutateMap(row)
	default:
		return errors.New(E_CONSTRAINT_VIOLATION)
	}
}

func RowMutate(original *map[string]interface{}, mutations []interface{}) error {
	// XXX(alexey): can we just run on original ?
	mutated := &map[string]interface{}{}
	copier.Copy(mutated, original)
	for _, mt := range mutations {
		mutation, err := NewMutation(mt.([]interface{}))
		if err != nil {
			return err
		}
		err = mutation.Mutate(mutated)
		if err != nil {
			return err
		}
	}
	copier.Copy(original, mutated)
	return nil
}

func RowUpdate(original *map[string]interface{}, update map[string]interface{}) error {
	for k, v := range update {
		switch k {
		case COL_UUID, COL_VERSION:
			return errors.New(E_CONSTRAINT_VIOLATION)
		}
		(*original)[k] = v
	}
	return nil
}

func etcdGetTable(txn *Transaction, table string) {
	prefix := makePrefix(txn.request.DBName, table)
	etcdOp := clientv3.OpGet(prefix, clientv3.WithPrefix())
	txn.etcdThen = append(txn.etcdThen, etcdOp)
}

func etcdGetRow(txn *Transaction, table, uuid string) {
	key := makeKey(txn.request.DBName, table, uuid)
	etcdOp := clientv3.OpGet(key)
	txn.etcdThen = append(txn.etcdThen, etcdOp)
}

func etcdGetTableOrRow(txn *Transaction, table, uuid string) {
	if uuid != "" {
		etcdGetRow(txn, table, uuid)
	} else {
		etcdGetTable(txn, table)
	}
}

func etcdGetByWhere(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	uuid, err := doesWhereContainCondTypeUUID(ovsOp.Where)
	if err != nil {
		ovsResult.Error = err.Error()
		return err
	}
	etcdGetTableOrRow(txn, ovsOp.Table, uuid)
	return nil
}

func etcdPutRow(txn *Transaction, table, uuid string, row *map[string]interface{}) error {
	key := makeKey(txn.request.DBName, table, uuid)
	setRowUUID(row, uuid)
	val, err := makeValue(row)
	if err != nil {
		return err
	}
	etcdOp := clientv3.OpPut(key, val)

	/* remove any duplicate keys from prev operations */
	newThen := []clientv3.Op{}
	for _, op := range txn.etcdThen {
		v := reflect.ValueOf(op)
		f := v.FieldByName("key")
		k := f.Bytes()
		if string(k) != key {
			newThen = append(newThen, op)
		}
	}
	txn.etcdThen = newThen

	txn.etcdThen = append(txn.etcdThen, etcdOp)
	return nil
}

func etcdDelRow(txn *Transaction, table, uuid string) error {
	key := makeKey(txn.request.DBName, table, uuid)
	etcdOp := clientv3.OpDelete(key)
	txn.etcdThen = append(txn.etcdThen, etcdOp)
	return nil
}

/* insert */
func preInsert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	if ovsOp.UUIDName == "" {
		return nil
	}
	etcdGetTable(txn, ovsOp.Table)
	return nil
}

func doInsert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	if ovsOp.UUIDName != "" {
		for _, row := range txn.data.Table(ovsOp.Table) {
			if (*row)[COL_UUIDNAME] == ovsOp.UUIDName {
				err := errors.New(E_DUP_UUIDNAME)
				ovsResult.Error = err.Error()
				return err
			}
		}
	}
	ovsResult.Count = ovsResult.Count + 1
	uuid := guuid.New().String() /* generate RFC4122 UUID */
	row := txn.data.Row(ovsOp.Table, uuid)
	*row = ovsOp.Row // supporting read-afrer-write
	return etcdPutRow(txn, ovsOp.Table, uuid, row)
}

/* select */
func preSelect(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doSelect(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	for _, row := range txn.data.Table(ovsOp.Table) {
		ok, err := isRowSelectedByWhere(row, ovsOp.Where)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		resultRow := reduceRowByColumns(row, ovsOp.Columns)
		ovsResult.Count = ovsResult.Count + 1
		ovsResult.Rows = append(ovsResult.Rows, *resultRow)
	}
	return nil
}

/* update */
func preUpdate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doUpdate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	for uuid, row := range txn.data.Table(ovsOp.Table) {
		ok, err := isRowSelectedByWhere(row, ovsOp.Where)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		ovsResult.Count = ovsResult.Count + 1
		err = RowUpdate(row, ovsOp.Row)
		if err != nil {
			return err
		}
		*(txn.data.Row(ovsOp.Table, uuid)) = *row
		etcdPutRow(txn, ovsOp.Table, uuid, row)
	}
	return nil
}

/* mutate */
func preMutate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doMutate(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	for uuid, row := range txn.data.Table(ovsOp.Table) {
		ok, err := isRowSelectedByWhere(row, ovsOp.Where)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		ovsResult.Count = ovsResult.Count + 1
		err = RowMutate(row, ovsOp.Mutations)
		if err != nil {
			return err
		}
		*(txn.data.Row(ovsOp.Table, uuid)) = *row
		etcdPutRow(txn, ovsOp.Table, uuid, row)
	}
	return nil
}

/* delete */
func preDelete(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doDelete(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	for uuid, row := range txn.data.Table(ovsOp.Table) {
		ok, err := isRowSelectedByWhere(row, ovsOp.Where)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		ovsResult.Count = ovsResult.Count + 1
		etcdDelRow(txn, ovsOp.Table, uuid)
	}
	return nil
}

/* wait */
func preWait(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	if ovsOp.Timeout != 0 {
		err := fmt.Errorf("only support timeout 0")
		ovsResult.Error = err.Error()
		return err
	}
	return etcdGetByWhere(txn, ovsOp, ovsResult)
}

func doWait(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	rows := []map[string]interface{}{}
	for _, row := range txn.data.Table(ovsOp.Table) {
		ok, err := isRowSelectedByWhere(row, ovsOp.Where)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		rows = append(rows, *(reduceRowByColumns(row, ovsOp.Columns)))
	}

	var err error
	equal := isEqualRows(rows, ovsOp.Rows)
	switch ovsOp.Until {
	case FN_EQ:
		if !equal {
			err = errors.New(E_TIMEOUT)
		}
	case FN_NE:
		if equal {
			err = errors.New(E_TIMEOUT)
		}
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
	}

	if err != nil {
		ovsResult.Error = err.Error()
	}
	return err
}

/* commit */
func preCommit(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	if ovsOp.Durable {
		err := errors.New(E_NOT_SUPPORTED)
		ovsResult.Error = err.Error()
		return err
	}
	return nil
}

func doCommit(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return nil
}

/* abort */
func preAbort(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	err := errors.New(E_ABORTED)
	ovsResult.Error = err.Error()
	return err
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
	key := makeKey(txn.request.DBName, "_comment", timestamp)
	comment := ovsOp.Comment
	etcdOp := clientv3.OpPut(key, comment)
	txn.etcdThen = append(txn.etcdThen, etcdOp)
	return nil
}

/* assert */
func preAssert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return errors.New(E_NOT_OWNER)
}

func doAssert(txn *Transaction, ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) error {
	return nil
}
