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
	"time"

	"github.com/go-logr/logr"
	"github.com/jinzhu/copier"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

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

// etcdTrx doesn't allow modification of the same key in a single transaction.
// we have to validate correctness of this operation removing.
func (txn *Transaction) etcdRemoveDupThen() {
	duplicatedKeys := map[int]int{}
	prevKeyIndex := map[string]int{}
	for curr, op := range txn.etcdTrx.Then {
		key := string(op.KeyBytes())
		prev, ok := prevKeyIndex[key]
		if ok {
			txn.log.V(6).Info("[then] removing duplicate key", "key", key, "index", prev)
			duplicatedKeys[prev] = prev
		}
		prevKeyIndex[key] = curr
	}
	newThen := []clientv3.Op{}
	for inx, op := range txn.etcdTrx.Then {
		if _, ok := duplicatedKeys[inx]; !ok {
			newThen = append(newThen, op)
		}
	}
	txn.etcdTrx.Then = newThen
}

func (txn *Transaction) etcdRemoveDup() {
	txn.etcdRemoveDupThen()
}

func (txn *Transaction) etcdTransaction() (*clientv3.TxnResponse, error) {
	txn.log.V(6).Info("etcdTrx transaction", "etcdTrx", txn.etcdTrx.String())
	errInternal := txn.etcdTrx.Commit()
	if errInternal != nil {
		err := errors.New(E_IO_ERROR)
		txn.log.Error(err, "etcdTrx transaction", "err", errInternal)
		return nil, err
	}
	return txn.etcdTrx.Res, nil
}

type namedUUIDResolver map[string]string

func (mapUUID *namedUUIDResolver) Set(uuidName, uuid string, log logr.Logger) {
	log.V(6).Info("set named-uuid", "uuid-name", uuidName, "uuid", uuid)
	(*mapUUID)[uuidName] = uuid
}

func (mapUUID *namedUUIDResolver) Get(uuidName string, log logr.Logger) (string, error) {
	uuid, ok := (*mapUUID)[uuidName]
	if !ok {
		err := errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "can't get named-uuid", "uuid-name", uuidName)
		return "", err
	}
	return uuid, nil
}

func (mapUUID *namedUUIDResolver) ResolveUUID(value interface{}, log logr.Logger) (interface{}, error) {
	namedUuid, _ := value.(libovsdb.UUID)
	if namedUuid.GoUUID != "" && namedUuid.ValidateUUID() != nil {
		uuid, err := mapUUID.Get(namedUuid.GoUUID, log)
		if err != nil {
			return nil, err
		}
		value = libovsdb.UUID{GoUUID: uuid}
	}
	return value, nil
}

func (mapUUID *namedUUIDResolver) ResolveSet(value interface{}, log logr.Logger) (interface{}, error) {
	oldset, _ := value.(libovsdb.OvsSet)
	newset := libovsdb.OvsSet{}
	for _, oldval := range oldset.GoSet {
		newval, err := mapUUID.ResolveUUID(oldval, log)
		if err != nil {
			return nil, err
		}
		newset.GoSet = append(newset.GoSet, newval)
	}
	return newset, nil
}

func (mapUUID *namedUUIDResolver) ResolveMap(value interface{}, log logr.Logger) (interface{}, error) {
	oldmap, _ := value.(libovsdb.OvsMap)
	newmap := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}
	for key, oldval := range oldmap.GoMap {
		newval, err := mapUUID.ResolveUUID(oldval, log)
		if err != nil {
			return nil, err
		}
		newmap.GoMap[key] = newval
	}
	return newmap, nil
}

func (mapUUID *namedUUIDResolver) Resolve(value interface{}, log logr.Logger) (interface{}, error) {
	switch value.(type) {
	case libovsdb.UUID:
		return mapUUID.ResolveUUID(value, log)
	case libovsdb.OvsSet:
		return mapUUID.ResolveSet(value, log)
	case libovsdb.OvsMap:
		return mapUUID.ResolveMap(value, log)
	default:
		return value, nil
	}
}

func (mapUUID *namedUUIDResolver) ResolveRow(row *map[string]interface{}, log logr.Logger) error {
	for column, value := range *row {
		value, err := mapUUID.Resolve(value, log)
		if err != nil {
			return err
		}
		(*row)[column] = value
	}
	return nil
}

type etcdTransaction struct {
	Cli  *clientv3.Client
	Ctx  context.Context
	If   []clientv3.Cmp
	Then []clientv3.Op
	Else []clientv3.Op
	Res  *clientv3.TxnResponse
}

func (etcd *etcdTransaction) Clear() {
	etcd.If = []clientv3.Cmp{}
	etcd.Then = []clientv3.Op{}
	etcd.Else = []clientv3.Op{}
	etcd.Res = nil
}

func (etcd etcdTransaction) String() string {
	return fmt.Sprintf("{txn-num-op=%d}", len(etcd.Then))
}

func (etcd *etcdTransaction) Commit() error {
	res, err := etcd.Cli.Txn(etcd.Ctx).If(etcd.If...).Then(etcd.Then...).Else(etcd.Else...).Commit()
	if err != nil {
		return err
	}
	etcd.Res = res
	return nil
}

// [table][uuid]row
type localCache map[string]localTableCache

// [full key]row
type localTableCache map[string]map[string]interface{}

func (lc *localCache) getLocalTableCache(tabelName string) localTableCache {
	table, ok := (*lc)[tabelName]
	if !ok {
		table = make(map[string]map[string]interface{})
		(*lc)[tabelName] = table
	}
	return table
}

type Transaction struct {
	/* logger */
	log logr.Logger

	/* ovs */
	schema   *libovsdb.DatabaseSchema
	request  libovsdb.Transact
	response libovsdb.TransactResponse

	/* cache */
	cache *databaseCache
	// per transaction cache [tableName]->[key]->value
	localCache *localCache
	mapUUID    namedUUIDResolver

	/* etcdTrx */
	etcdTrx *etcdTransaction

	/* session for tables locks */
	session *concurrency.Session
}

func NewTransaction(ctx context.Context, cli *clientv3.Client, request *libovsdb.Transact, cache *databaseCache,
	schema *libovsdb.DatabaseSchema, log logr.Logger) (*Transaction, error) {
	txn := new(Transaction)
	txn.log = log.WithValues()
	txn.log.V(5).Info("new transaction", "size", len(request.Operations), "request", request)
	txn.cache = cache
	txn.mapUUID = namedUUIDResolver{}
	txn.schema = schema
	txn.request = *request
	txn.localCache = &localCache{}
	txn.response.Result = make([]libovsdb.OperationResult, len(request.Operations))
	txn.etcdTrx = &etcdTransaction{Ctx: ctx, Cli: cli}
	return txn, nil
}

func (txn *Transaction) generateUUID(ovsOp *libovsdb.Operation) (string, error) {
	var uuid string
	tCache := txn.cache.getTable(*ovsOp.Table)
	if ovsOp.UUID != nil {
		uuid = ovsOp.UUID.GoUUID
		if _, ok := (*tCache)[uuid]; ok {
			err := errors.New(E_DUP_UUID)
			txn.log.Error(err, "duplicate uuid", "uuid", uuid)
			return "", err
		}
	} else {
		uuid = common.GenerateUUID()
		for ok := true; ok; {
			_, ok = (*tCache)[uuid]
		}
	}
	return uuid, nil
}

func (txn *Transaction) Commit() (int64, error) {
	txn.log.V(5).Info("commit transaction")
	var err error

	/* verify that "select" is not intermixed with write operations */
	hasSelect := false
	hasWrite := false
Loop:
	for _, ovsOp := range txn.request.Operations {
		switch ovsOp.Op {
		case libovsdb.OperationSelect:
			hasSelect = true
			if hasWrite {
				break Loop
			}
		case libovsdb.OperationDelete, libovsdb.OperationMutate, libovsdb.OperationUpdate, libovsdb.OperationInsert:
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

	processOperations := func() error {
		txn.cache.mu.Lock()
		defer txn.cache.mu.Unlock()
		// insert name-uuid preprocessing
		for i, ovsOp := range txn.request.Operations {
			if ovsOp.Op == libovsdb.OperationInsert {
				if ovsOp.UUIDName != nil {
					if _, ok := txn.mapUUID[*ovsOp.UUIDName]; ok {
						err = errors.New(E_DUP_UUIDNAME)
						txn.log.Error(err, "", "uuid-name", *ovsOp.UUIDName)
						txn.response.Result[i].SetError(E_DUP_UUIDNAME)
						// we will return error for the operation processing
					} else {
						// TODO
						var uuid string
						if ovsOp.UUID != nil {
							uuid = ovsOp.UUID.GoUUID
						} else {
							uuid, err = txn.generateUUID(&ovsOp)
							if err != nil {
								return err
							}
						}
						txn.mapUUID.Set(*ovsOp.UUIDName, uuid, txn.log)
					}
				}
			}
		}

		for i, ovsOp := range txn.request.Operations {
			err = txn.doOperation(&ovsOp, &txn.response.Result[i])
			if err != nil {
				errStr := err.Error()
				txn.response.Result[i].SetError(errStr)
				//txn.response.Error = &errStr
				return err
			}
		}
		return nil
	}
	err = processOperations()
	if err != nil {
		return -1, err
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

func (txn *Transaction) doOperation(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error) {
	txn.log.V(6).Info("start operation", "op", ovsOp.String())
	defer txn.log.V(6).Info("end operation", "result", ovsResult, "error", err)
	var details string
	switch ovsOp.Op {
	case libovsdb.OperationInsert:
		err, details = txn.validateOpsTableName(ovsOp.Table)
		if err == nil {
			err, details = txn.doInsert(ovsOp, ovsResult)
		}
	case libovsdb.OperationSelect:
		err, details = txn.validateOpsTableName(ovsOp.Table)
		if err == nil {
			ovsResult.InitRows()
			err, details = txn.doSelectDelete(ovsOp, ovsResult)
		}
	case libovsdb.OperationUpdate, libovsdb.OperationMutate:
		err, details = txn.validateOpsTableName(ovsOp.Table)
		if err == nil {
			err, details = txn.doModify(ovsOp, ovsResult)
		}
	case libovsdb.OperationDelete:
		err, details = txn.validateOpsTableName(ovsOp.Table)
		if err == nil {
			ovsResult.InitCount()
			err, details = txn.doSelectDelete(ovsOp, ovsResult)
		}
	case libovsdb.OperationWait:
		err, details = txn.validateOpsTableName(ovsOp.Table)
		if err == nil {
			err, details = txn.doWait(ovsOp, ovsResult)
		}
	case libovsdb.OperationCommit:
		err, details = txn.doCommit(ovsOp, ovsResult)
	case libovsdb.OperationAbort:
		err, details = txn.doAbort(ovsOp, ovsResult)
	case libovsdb.OperationComment:
		err, details = txn.doComment(ovsOp, ovsResult)
	case libovsdb.OperationAssert:
		err, details = txn.doAssert(ovsOp, ovsResult)
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "wrong operation", "operation", ovsOp.Op)
	}
	if err != nil {
		ovsResult.SetError(err.Error())
		if details != "" {
			ovsResult.Details = &details
		}
	}
	return err
}

func (txn *Transaction) conditionsFromWhere(op *libovsdb.Operation) (Conditions, error) {
	if op.Where == nil {
		return make([]Condition, 0, 0), nil
	}
	tableSchema, err := txn.schema.LookupTable(*op.Table)
	if err != nil {
		return nil, err
	}
	conditions := make([]Condition, len(*op.Where), len(*op.Where))
	for j, c := range *op.Where {
		cond, ok := c.([]interface{})
		if !ok {
			err := errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "failed to convert condition value", "condition", c)
			return nil, err
		}
		condition, err := NewCondition(tableSchema, txn.mapUUID, cond, txn.log)
		if err != nil {
			return nil, err
		}
		conditions[j] = *condition
	}
	return conditions, nil
}

func (txn *Transaction) lockTables() error {
	var tables []string
	tMap := make(map[string]bool)
	for _, op := range txn.request.Operations {
		switch op.Op {
		case libovsdb.OperationInsert, libovsdb.OperationUpdate, libovsdb.OperationMutate, libovsdb.OperationDelete, libovsdb.OperationSelect:
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
		session, err := concurrency.NewSession(txn.etcdTrx.Cli, concurrency.WithContext(txn.etcdTrx.Ctx))
		if err != nil {
			return err
		}
		txn.session = session
		for _, table := range tables {
			key := common.NewLockKey("__" + table)
			mutex := concurrency.NewMutex(session, key.String())
			mutex.Lock(txn.etcdTrx.Ctx)
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

func (txn *Transaction) getUUIDIfExists(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, cond1 interface{}) (string, error) {
	var err error
	cond2, ok := cond1.([]interface{})
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed to convert condition", "condition", cond1)
		return "", err
	}
	condition, err := NewCondition(tableSchema, mapUUID, cond2, txn.log)
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

func (txn *Transaction) validateOpsTableName(tableName *string) (error, string) {
	if tableName == nil || *tableName == " " {
		err := errors.New(E_CONSTRAINT_VIOLATION)
		msg := "missing table name"
		txn.log.Error(err, msg)
		return err, msg
	}
	return nil, ""
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

func (txn *Transaction) RowMutate(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, oldRow *map[string]interface{}, mutations *[]interface{}) (*map[string]interface{}, error) {
	newRow := &map[string]interface{}{}
	copier.Copy(newRow, oldRow)
	err := tableSchema.Unmarshal(newRow)
	if err != nil {
		return nil, err
	}
	for _, mt := range *mutations {
		m, err := NewMutation(tableSchema, mapUUID, mt.([]interface{}), txn.log)
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

func (txn *Transaction) RowUpdate(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, currentRow *map[string]interface{}, update *map[string]interface{}) (*map[string]interface{}, error) {
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

func (txn *Transaction) etcdModifyRow(key []byte, row *map[string]interface{}) error {
	comKey, err := common.ParseKey(string(key))
	if err != nil {
		return err
	}
	val, err := makeValue(row)
	if err != nil {
		return err
	}
	table := txn.localCache.getLocalTableCache(comKey.TableName)
	table[string(key)] = *row
	etcdOp := clientv3.OpPut(string(key), val)
	txn.etcdTrx.Then = append(txn.etcdTrx.Then, etcdOp)
	return nil
}

func (txn *Transaction) RowPrepare(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, row *map[string]interface{}) error {
	err := tableSchema.Unmarshal(row)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed to unmarshal row")
		return err
	}

	err = mapUUID.ResolveRow(row, txn.log)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed to resolve uuid of row")
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
func (txn *Transaction) doInsert(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error, details string) {
	// we check for an error that can be created during the named-uuid preprocessing
	if ovsResult.Error != nil {
		return fmt.Errorf(*ovsResult.Error), ""
	}
	tableSchema, e := txn.schema.LookupTable(*ovsOp.Table)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}

	var uuid string
	if ovsOp.UUIDName != nil {
		uuid, err = txn.mapUUID.Get(*ovsOp.UUIDName, txn.log)
		if err != nil {
			txn.log.Error(err, "can't find uuid-name", "uuid-name", *ovsOp.UUIDName)
			return
		}
		if ovsOp.UUID != nil && (*ovsOp.UUID).GoUUID != uuid {
			err = errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "mismatching uuid-name and uuid", "uuid-name", *ovsOp.UUIDName, "uuid", uuid)
			return
		}
	}
	if uuid == "" {
		uuid, err = txn.generateUUID(ovsOp)
		if err != nil {
			return
		}
	}
	ovsResult.InitUUID(uuid)

	key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
	row := &map[string]interface{}{}

	*row = *ovsOp.Row
	txn.schema.Default(*ovsOp.Table, row)

	err = txn.RowPrepare(tableSchema, txn.mapUUID, ovsOp.Row)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "failed to prepare row", "row", row)
		return
	}
	setRowUUID(row, uuid)
	setRowVersion(row)
	//err = etcdCreateRow(txn, &key, row)
	k := key.String()
	val, e := makeValue(row)
	if e != nil {
		err = e
		return
	}
	table := txn.localCache.getLocalTableCache(*ovsOp.Table)
	table[k] = *row
	etcdOp := clientv3.OpPut(k, val)
	txn.etcdTrx.Then = append(txn.etcdTrx.Then, etcdOp)
	return
}

/* update and mutate */
func (txn *Transaction) doModify(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error, details string) {
	ovsResult.InitCount()
	tableSchema, e := txn.schema.LookupTable(*ovsOp.Table)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}
	tCache := txn.cache.getTable(*ovsOp.Table)
	lCache := txn.localCache.getLocalTableCache(*ovsOp.Table)
	cond, e := txn.conditionsFromWhere(ovsOp)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}

	rowProcess := func(key []byte, row map[string]interface{}) {
		var newRow *map[string]interface{}
		if ovsOp.Op == libovsdb.OperationUpdate {
			err = txn.RowPrepare(tableSchema, txn.mapUUID, ovsOp.Row)
			if err != nil {
				txn.log.Error(err, "failed to prepare row", "row", ovsOp.Row)
				return
			}
			newRow, err = txn.RowUpdate(tableSchema, txn.mapUUID, &row, ovsOp.Row)
			if err != nil {
				txn.log.Error(err, "failed to update row", "row", ovsOp.Row)
				return
			}
		} else if ovsOp.Op == libovsdb.OperationMutate {
			newRow, err = txn.RowMutate(tableSchema, txn.mapUUID, &row, ovsOp.Mutations)
			if err != nil {
				txn.log.Error(err, "failed to row mutate", "row", row, "mutations", ovsOp.Mutations)
				return
			}
		}
		setRowVersion(newRow)
		err = txn.etcdModifyRow(key, newRow)
		if err != nil {
			return
		}
		ovsResult.IncrementCount()
	}

	uuid, e := cond.getUUIDIfSelected()
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}
	cache := lCache
	if uuid != "" {
		key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
		keyS := key.String()
		row, ok := lCache[keyS]
		cache = localTableCache{}
		if ok {
			cache[keyS] = row
		}
	}
	for key, row := range cache {
		var ok bool
		ok, err = cond.isRowSelected(&row)
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		rowProcess([]byte(key), row)
	}

	if uuid != "" {
		kv, ok := (*tCache)[uuid]
		tCache = &tableCache{}
		if ok {
			(*tCache)[uuid] = kv
		}
	}
	for _, kv := range *tCache {
		_, ok := lCache[string(kv.Key)]
		if ok {
			// the row was modified by this transaction, we handled it above
			continue
		}
		row := map[string]interface{}{}
		err = json.Unmarshal(kv.Value, &row)
		if err != nil {
			txn.log.Error(err, "failed to unmarshal row", "kv.Key", string(kv.Key), "kv.Value", string(kv.Value))
			return
		}
		if uuid == "" || len(cond) > 1 {
			// there are other conditions in addition or instead of _uuid
			ok, err = cond.isRowSelected(&row)
			if err != nil {
				return
			}
			if !ok {
				continue
			}
		}
		rowProcess(kv.Key, row)
	}
	return
}

/* Select and Delete */
func (txn *Transaction) doSelectDelete(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error, details string) {
	tCache := txn.cache.getTable(*ovsOp.Table)
	cond, e := txn.conditionsFromWhere(ovsOp)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}
	uuid, e := cond.getUUIDIfSelected()
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}

	if uuid != "" {
		kv, ok := (*tCache)[uuid]
		if !ok {
			return
		}
		tCache = &tableCache{uuid: kv}
	}
	for _, kv := range *tCache {
		row := map[string]interface{}{}
		err = json.Unmarshal(kv.Value, &row)
		if err != nil {
			txn.log.Error(err, "failed to unmarshal row", "kv.Key", string(kv.Key), "kv.Value", string(kv.Value))
			return
		}
		if uuid == "" || len(cond) > 1 {
			// there are other conditions in addition or instead of _uuid
			ok, e := cond.isRowSelected(&row)
			if e != nil {
				err = errors.New(E_INTERNAL_ERROR)
				return
			}
			if !ok {
				continue
			}
		}
		if ovsOp.Op == libovsdb.OperationDelete {
			etcdOp := clientv3.OpDelete(string(kv.Key))
			txn.etcdTrx.Then = append(txn.etcdTrx.Then, etcdOp)
			ovsResult.IncrementCount()
		} else if ovsOp.Op == libovsdb.OperationSelect {
			resultRow, e := reduceRowByColumns(&row, ovsOp.Columns)
			if e != nil {
				txn.log.Error(err, "failed to reduce row by columns", "row", row, "columns", ovsOp.Columns)
				err = e
				return
			}
			ovsResult.AppendRows(*resultRow)
		} else {
			txn.log.Error(err, "wrong operation", "operation", ovsOp.Op)
			err = errors.New(E_INTERNAL_ERROR)
		}
	}
	return
}

/* wait */
func (txn *Transaction) doWait(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error, details string) {
	if ovsOp.Timeout == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		details = "missing timeout parameter"
		txn.log.Error(err, details)
		return
	}
	if *ovsOp.Timeout != 0 {
		txn.log.V(5).Info("ignoring non-zero wait timeout", "timeout", *ovsOp.Timeout)
	}
	if ovsOp.Rows == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		details = "missing rows parameter"
		txn.log.Error(err, details)
	}
	if len(*ovsOp.Rows) == 0 {
		return
	}
	if ovsOp.Until == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		details = "missing until parameter"
		txn.log.Error(err, details)
		return
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
		details = "wrong until operator: " + *ovsOp.Until
		return
	}

	tableSchema, errInternal := txn.schema.LookupTable(*ovsOp.Table)
	if errInternal != nil {
		err = errors.New(E_INTERNAL_ERROR)
		txn.log.Error(err, "failed table schema lookup", "err", errInternal.Error())
		details = "missing table schema"
	}

	tCache := txn.cache.getTable(*ovsOp.Table)
	cond, e := txn.conditionsFromWhere(ovsOp)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}
	uuid, e := cond.getUUIDIfSelected()
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}

	if uuid != "" {
		kv, ok := (*tCache)[uuid]
		if !ok {
			return
		}
		tCache = &tableCache{uuid: kv}
	}
	for _, kv := range *tCache {
		actualRow := &map[string]interface{}{}
		err = json.Unmarshal(kv.Value, &actualRow)
		if err != nil {
			txn.log.Error(err, "failed to unmarshal row", "kv.Key", string(kv.Key), "kv.Value", string(kv.Value))
			return
		}
		if uuid == "" || len(cond) > 1 {
			// there are other conditions in addition or instead of _uuid
			ok, e := cond.isRowSelected(actualRow)
			if e != nil {
				err = errors.New(E_INTERNAL_ERROR)
				return
			}
			if !ok {
				continue
			}
		}
		err = tableSchema.Unmarshal(actualRow)
		if err != nil {
			return
		}
		if ovsOp.Columns != nil {
			actualRow, err = reduceRowByColumns(actualRow, ovsOp.Columns)
			if err != nil {
				txn.log.Error(err, "failed to reduce row by columns", "row", actualRow, "columns", ovsOp.Columns)
				return
			}
		}

		for _, expected := range *ovsOp.Rows {
			err = txn.RowPrepare(tableSchema, txn.mapUUID, &expected)
			if err != nil {
				return
			}
			cond, e := isEqualRow(txn, tableSchema, &expected, actualRow)
			txn.log.V(5).Info("checking row equal", "expected", expected, "actual", actualRow, "result", cond)
			if e != nil {
				txn.log.Error(err, "error in row compare", "expected", expected)
				err = errors.New(E_INTERNAL_ERROR)
				return
			}
			if cond {
				if equal {
					return
				}
				err = errors.New(E_TIMEOUT)
				details = "timed out"
				txn.log.Error(err, details)
				return
			}
		}
	}
	if !equal {
		return
	}
	err = errors.New(E_TIMEOUT)
	details = "timed out"
	txn.log.Error(err, details)
	return
}

func (txn *Transaction) doCommit(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error, details string) {
	if ovsOp.Durable == nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		txn.log.Error(err, "missing durable parameter")
		return
	}
	if *ovsOp.Durable {
		err = errors.New(E_NOT_SUPPORTED)
		txn.log.Error(err, "do not support durable == true")
		return
	}
	return
}

/* abort */
func (txn *Transaction) doAbort(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (error, string) {
	return errors.New(E_ABORTED), ""
}

/* comment */
func (txn *Transaction) doComment(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (error, string) {
	timestamp := time.Now().Format(time.RFC3339)
	key := common.NewCommentKey(txn.request.DBName, timestamp)
	comment := *ovsOp.Comment
	etcdOp := clientv3.OpPut(key.String(), comment)
	txn.etcdTrx.Then = append(txn.etcdTrx.Then, etcdOp)
	return nil, ""
}

/* assert */
func (txn *Transaction) doAssert(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (error, string) {
	return nil, ""
}
