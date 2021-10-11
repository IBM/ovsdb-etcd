package ovsdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"github.com/jinzhu/copier"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

const (
	/* ovsdb operations */
	E_DUP_UUID_NAME        = "duplicate uuid-name"
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

	/* ovsdb extension */
	E_DUP_UUID          = "duplicate uuid"
	E_INTERNAL_ERROR    = "internal error"
	E_CONCURRENCY_ERROR = "concurrency error"
	E_OVSDB_ERROR       = "ovsdb error"
	E_PERMISSION_ERROR  = "permission error"
	E_SYNTAX_ERROR      = "syntax error or unknown column"
)

func isEqualColumn(columnSchema *libovsdb.ColumnSchema, expected, actual interface{}, log logr.Logger) bool {
	var ok bool
	switch columnSchema.Type {
	case libovsdb.TypeSet:
		expectedSet, ok1 := expected.(libovsdb.OvsSet)
		actualSet, ok2 := actual.(libovsdb.OvsSet)
		if ok1 && ok2 {
			ok = libovsdb.IsEqualSets(expectedSet, actualSet)
		} else if !ok1 && ok2 {
			ok = actualSet.EqualsToValue(expected)
		} else if !ok2 && ok1 {
			ok = expectedSet.EqualsToValue(actual)
		} else {
			ok = reflect.DeepEqual(expected, actual)
		}
	case libovsdb.TypeMap:
		expectedMap := expected.(libovsdb.OvsMap)
		actualMap := actual.(libovsdb.OvsMap)
		ok =  libovsdb.IsEqualMaps(expectedMap, actualMap)
	default:
		ok = reflect.DeepEqual(expected, actual)
	}
	if !ok {
		log.V(5).Info("isEqualColumn return false", "column type", columnSchema.Type,
			"expected", fmt.Sprintf("%T %v", expected, expected),
			"actual",  fmt.Sprintf("%T %v",  actual, actual))
	}
	return ok
}

func (txn *Transaction)isEqualRow(tableSchema *libovsdb.TableSchema, expectedRow, actualRow *map[string]interface{}) (bool, error) {
	for column, expected := range *expectedRow {
		columnSchema, err := tableSchema.LookupColumn(column)
		if err != nil {
			err := errors.New(E_CONSTRAINT_VIOLATION)
			txn.log.Error(err, "schema doesn't contain column", "column", column)
			return false, err
		}
		actual := (*actualRow)[column]
		if !isEqualColumn(columnSchema, expected, actual, txn.log ) {
			return false, nil
		}
	}
	return true, nil
}

func (txn *Transaction) etcdTransaction() (*clientv3.TxnResponse, error) {
	txn.etcdTrx.removeDupThen()
	txn.log.V(6).Info("etcdTrx transaction", "if", txn.etcdTrx.ifSize(), "then", txn.etcdTrx.thenSize())
	return txn.etcdTrx.commit()
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
	oldSet, _ := value.(libovsdb.OvsSet)
	newSet := libovsdb.OvsSet{}
	for _, oldVal := range oldSet.GoSet {
		newVal, err := mapUUID.ResolveUUID(oldVal, log)
		if err != nil {
			return nil, err
		}
		newSet.GoSet = append(newSet.GoSet, newVal)
	}
	return newSet, nil
}

func (mapUUID *namedUUIDResolver) ResolveMap(value interface{}, log logr.Logger) (interface{}, error) {
	oldMap, _ := value.(libovsdb.OvsMap)
	newMap := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}
	for key, oldVal := range oldMap.GoMap {
		newVal, err := mapUUID.ResolveUUID(oldVal, log)
		if err != nil {
			return nil, err
		}
		newMap.GoMap[key] = newVal
	}
	return newMap, nil
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

// table->uuid->count
type refCounter map[string]map[string]int

func (rc refCounter) updateCounters (tableName string, newCounters map[string]int) {
	if newCounters == nil || len(newCounters) == 0 {
		return
	}
	table, ok := rc[tableName]
	if !ok {
		rc[tableName] = newCounters
		return
	}
	for uuid, count := range newCounters {
		oldCounter, ok := table[uuid]
		if !ok {
			table[uuid] = count
		} else {
			table[uuid] = oldCounter + count
		}
	}
}

type etcdTransaction struct {
	cli  *clientv3.Client
	ctx  context.Context
	cmp  []clientv3.Cmp
	then []clientv3.Op
	// we don't use else operations, so we don't have the else entry here
}

func (etcd *etcdTransaction) clear() {
	etcd.cmp = []clientv3.Cmp{}
	etcd.then = []clientv3.Op{}
}

func (etcd *etcdTransaction) appendIf(cmp clientv3.Cmp) {
	etcd.cmp = append(etcd.cmp, cmp)
}

func (etcd *etcdTransaction) appendThen(op clientv3.Op) {
	etcd.then = append(etcd.then, op)
}

func (etcd etcdTransaction) String() string {
	ifStr := "if: {"
	if etcd.cmp != nil {
		for _, i := range etcd.cmp {
			cmp := etcdserverpb.Compare(i)
			ifStr += fmt.Sprintf("{%s},", cmp.String())
		}
	}
	ifStr += "}"
	thenStr := "then:{"
	if etcd.then != nil {
		for _, t := range etcd.then {
			thenStr += fmt.Sprintf("{key:%s, delete: %v, put: %v, get: %v },",
				string(t.KeyBytes()),
				t.IsDelete(),
				t.IsPut(),
				t.IsGet())
		}
	}
	thenStr += "}"
	return fmt.Sprintf("etcdTransaction: %d->%d, %s %s", etcd.ifSize(), etcd.thenSize(), ifStr, thenStr)
}

func (etcd etcdTransaction) ifSize() int {
	return len(etcd.cmp)
}

func (etcd etcdTransaction) thenSize() int {
	return len(etcd.then)
}

func (etcd *etcdTransaction) commit() (*clientv3.TxnResponse, error) {
	return etcd.cli.Txn(etcd.ctx).If(etcd.cmp...).Then(etcd.then...).Commit()
}

// etcdTrx doesn't allow modification of the same key in a single transaction.
// we have to validate correctness of this operation removing.
func (etcd *etcdTransaction) removeDupThen() {
	duplicatedKeys := map[int]int{}
	prevKeyIndex := map[string]int{}
	for curr, op := range etcd.then {
		key := string(op.KeyBytes())
		prev, ok := prevKeyIndex[key]
		if ok {
			duplicatedKeys[prev] = prev
		}
		prevKeyIndex[key] = curr
	}
	newThen := make([]clientv3.Op, 0, len(etcd.then))
	for inx, op := range etcd.then {
		if _, ok := duplicatedKeys[inx]; !ok {
			newThen = append(newThen, op)
		}
	}
	etcd.then = newThen
}

// [table][uuid]row
type localCache map[string]localTableCache

// [full key]row
type localTableCache map[string]map[string]interface{}

func (lc *localCache) getLocalTableCache(tableName string) localTableCache {
	table, ok := (*lc)[tableName]
	if !ok {
		table = make(map[string]map[string]interface{})
		(*lc)[tableName] = table
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
	refCounter refCounter
	/* etcdTrx */
	etcdTrx *etcdTransaction

	/* session for tables locks */
	session *concurrency.Session
}

func NewTransaction(ctx context.Context, cli *clientv3.Client, request *libovsdb.Transact, cache *databaseCache, schema *libovsdb.DatabaseSchema, log logr.Logger) (*Transaction, error) {
	txn := new(Transaction)
	txn.log = log.WithValues()
	txn.log.V(5).Info("new transaction", "size", len(request.Operations), "request", request)
	txn.cache = cache
	txn.mapUUID = namedUUIDResolver{}
	txn.refCounter = refCounter{}
	txn.schema = schema
	txn.request = *request
	txn.localCache = &localCache{}
	txn.response.Result = make([]libovsdb.OperationResult, len(request.Operations))
	txn.etcdTrx = &etcdTransaction{ctx: ctx, cli: cli}
	err := txn.conditionsFromWhere()
	if err != nil {
		return nil, err
	}
	return txn, nil
}

func (txn *Transaction) reset() {
	txn.etcdTrx.clear()
	txn.response.Result = make([]libovsdb.OperationResult, len(txn.request.Operations))
	txn.localCache = &localCache{}
	txn.refCounter = refCounter{}
}

func (txn *Transaction) gc() {
	txn.log.V(5).Info("gc", "refCounter", txn.refCounter )
	rc := txn.refCounter
	txn.refCounter = refCounter{}
	for table, val := range rc {
		tCache := txn.cache.getTable(table)
		for uuid, count := range val {
			cRow, ok := tCache.rows[uuid]
			if !ok {
				txn.log.V(1).Info("reference to non existing row", "table", table, "uuid", uuid)
			} else{
				if cRow.counter + count == 0 {
					// TODO check and remove references from this value
					etcdOp := clientv3.OpDelete(string(cRow.key))
					txn.etcdTrx.appendThen(etcdOp)
					for cName, destTable := range tCache.refColumns {
						val := cRow.row.Fields[cName]
						if val == nil {
							continue
						}
						cSchema, _ := txn.schema.LookupColumn(table, cName)
						txn.refCounter.updateCounters(destTable, checkCounters(nil, val, cSchema.Type))
					}

				}
			}
		}
	}
	if len(txn.refCounter) > 0 {
		txn.gc()
	}
}

func (txn *Transaction) getGenerateUUID(ovsOp *libovsdb.Operation) (string, error) {
	var uuid string
	if ovsOp.UUID != nil {
		uuid = ovsOp.UUID.GoUUID
		// TODO check duplications
	} else {
		uuid = common.GenerateUUID()
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
		txn.cache.mu.RLock()
		defer txn.cache.mu.RUnlock()
		// insert name-uuid preprocessing
		for i, ovsOp := range txn.request.Operations {
			if ovsOp.Op == libovsdb.OperationInsert {
				if ovsOp.UUIDName != nil {
					if _, ok := txn.mapUUID[*ovsOp.UUIDName]; ok {
						err = errors.New(E_DUP_UUID_NAME)
						txn.log.Error(err, "", "uuid-name", *ovsOp.UUIDName)
						txn.response.Result[i].SetError(E_DUP_UUID_NAME)
						// we will return error for the operation processing
						break
					} else {
						uuid, err := txn.getGenerateUUID(&ovsOp)
						if err != nil {
							return err
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
		txn.gc()
		return nil
	}
	var trResponse *clientv3.TxnResponse
	// TODO add configuration flag to iteration number
	for i := 0; i < 10; i++ {
		err = processOperations()
		if err != nil {
			return -1, err
		}
		trResponse, err = txn.etcdTransaction()
		if err != nil {
			txn.log.Error(err, "etcd trx error", "cmpSize", txn.etcdTrx.ifSize(), "thenSize", txn.etcdTrx.thenSize())
			return -1, err
		}
		if trResponse.Succeeded {
			if i > 0 {
				txn.log.V(6).Info("concurrency resolved", "trx", txn.etcdTrx.String())
			}
			break
		}
		txn.log.V(5).Info(E_CONCURRENCY_ERROR, "trx", txn.etcdTrx.String())
		// let's try again
		txn.reset()
		time.Sleep(time.Duration(i*5) * time.Millisecond)
	}
	if !trResponse.Succeeded {
		return -1, errors.New(E_INTERNAL_ERROR)
	}
	return trResponse.Header.Revision, nil
}

func (txn *Transaction) doOperation(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error) {
	txn.log.V(6).Info("start operation", "op", ovsOp.String())
	defer txn.log.V(6).Info("end operation", "result", ovsResult, "e", err)
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

/*
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
*/
func (txn *Transaction) conditionsFromWhere() error {
	for opIdx := range txn.request.Operations {
		op := &txn.request.Operations[opIdx]
		if op.Where == nil {
			continue
		}
		tableSchema, err := txn.schema.LookupTable(*op.Table)
		if err != nil {
			return err
		}
		conditions := make([]interface{}, len(*op.Where), len(*op.Where))
		for j, c := range *op.Where {
			cond, ok := c.([]interface{})
			if !ok {
				err := errors.New(E_INTERNAL_ERROR)
				txn.log.Error(err, "failed to convert condition value", "condition", c)
				return err
			}
			condition, err := NewCondition(tableSchema, cond, txn.log)
			if err != nil {
				return err
			}
			conditions[j] = *condition
		}
		op.Where = &conditions
	}
	return nil
}

func (txn *Transaction) updateConditions(ovsOp *libovsdb.Operation) (Conditions, error) {
	if ovsOp.Where == nil {
		return make(Conditions, 0, 0), nil
	}
	conditions := make(Conditions, len(*ovsOp.Where), len(*ovsOp.Where))
	for i, c := range *ovsOp.Where {
		cond, ok := c.(Condition)
		if !ok {
			err := errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "Cannot convert \"Where\" to Condition", "condition", c, "type", fmt.Sprintf("%T", c))
			return nil, err
		}
		err := cond.updateNamedUUID(txn.mapUUID, txn.log)
		if err != nil {
			txn.log.Error(err, "Cannot update NamedUUID")
			return nil, err
		}
		conditions[i] = cond
	}
	return conditions, nil
}

func (txn *Transaction) isSpecificRowSelected(ovsOp *libovsdb.Operation) (bool, error) {
	if ovsOp.Where == nil {
		return false, nil
	}
	tableSchema, e := txn.schema.LookupTable(*ovsOp.Table)
	if e != nil {
		return false, e
	}
	selectedColumns := map[string]string{}
	for _, c := range *ovsOp.Where {
		cond, ok := c.(Condition)
		if !ok {
			err := errors.New(E_INTERNAL_ERROR)
			txn.log.Error(err, "failed to convert condition value", "condition", c)
			return false, err
		}
		if cond.Function == FN_EQ || cond.Function == FN_IN {
			if cond.Column == libovsdb.COL_UUID {
				return true, nil
			}
			selectedColumns[cond.Column] = cond.Column
		}
	}
	if len(selectedColumns) > 0 {
		indexes := tableSchema.Indexes
		for _, indx := range indexes {
			indexed := true
			for _, col := range indx {
				if _, ok := selectedColumns[col]; !ok {
					indexed = false
					break
				}
			}
			if indexed {
				txn.log.V(6).Info("SpecificRowSelected by index", "index", indx)
				return true, nil
			}
		}
	}
	return false, nil
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

/*func (txn *Transaction) getUUIDIfExists(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, cond1 interface{}) (string, error) {
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
}*/

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

func (txn *Transaction) rowMutate(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, oldRow *map[string]interface{}, mutations *[]interface{}) (newRow *map[string]interface{}, err error) {
	newRow = &map[string]interface{}{}
	var errorMsg string
	defer func() {
		if err != nil {
			txn.log.Error(err, errorMsg)
			newRow = nil
		}
	}()
	if err = copier.Copy(newRow, oldRow); err != nil {
		errorMsg = "copier returned"
		return
	}
	if err = tableSchema.Unmarshal(newRow); err != nil {
		errorMsg = "unmarshal newRow"
		return
	}
	var mut *Mutation
	for _, mt := range *mutations {
		mut, err = NewMutation(tableSchema, mapUUID, mt.([]interface{}), txn.log)
		if err != nil {
			errorMsg = "new mutation"
			return
		}
		err = mut.Mutate(newRow)
		if err != nil {
			errorMsg = "mutate"
			return
		}
	}
	return
}

func columnUpdateMap(oldValue, newValue interface{}) (interface{}, error) {
	oldMap := oldValue.(libovsdb.OvsMap)
	newMap := newValue.(libovsdb.OvsMap)
	retMap := libovsdb.OvsMap{}
	if err := copier.Copy(&retMap, &oldMap); err != nil {
		return nil, err
	}
	for k, v := range newMap.GoMap {
		retMap.GoMap[k] = v
	}
	return retMap, nil
}

func columnUpdateSet(oldValue, newValue interface{}) (interface{}, error) {
	oldSet := oldValue.(libovsdb.OvsSet)
	newSet := newValue.(libovsdb.OvsSet)
	retSet := libovsdb.OvsSet{}
	if err := copier.Copy(&retSet, &oldSet); err != nil {
		return nil, err
	}
	for _, v := range newSet.GoSet {
		if !inSet(&oldSet, v) {
			retSet.GoSet = append(retSet.GoSet, v)
		}
	}
	return retSet, nil
}

func columnUpdate(columnSchema *libovsdb.ColumnSchema, oldValue, newValue interface{}) (interface{}, error) {
	switch columnSchema.Type {
	case libovsdb.TypeMap:
		return columnUpdateMap(oldValue, newValue)
	default:
		return newValue, nil
	}
}

func (txn *Transaction) rowUpdate(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, currentRow *map[string]interface{}, update *map[string]interface{}) (*map[string]interface{}, error) {
	// TODO should we try to map nameUUIDs ?
	newRow := new(map[string]interface{})
	if err := copier.Copy(newRow, currentRow); err != nil {
		txn.log.Error(err, "copier returned")
		return nil, err
	}
	if err := tableSchema.Unmarshal(newRow); err != nil {
		txn.log.Error(err, "unmarshal newRow")
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
			txn.log.Error(err, "failed update of immutable column", "column", column)
			return nil, err
		}

		val, err := columnUpdate(columnSchema, (*newRow)[column], value)
		if err != nil {
			txn.log.Error(err, "failed to update column", "column", column)
			return nil, err
		}
		(*newRow)[column] = val
	}
	return newRow, nil
}

func (txn *Transaction) etcdModifyRow(key []byte, row *map[string]interface{}, version int64) error {
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
	strKey := string(key)
	etcdOp := clientv3.OpPut(strKey, val)
	txn.etcdTrx.appendThen(etcdOp)
	if version != -1 {
		cmp := clientv3.Compare(clientv3.Version(strKey), "=", version)
		txn.etcdTrx.appendIf(cmp)
	}
	return nil
}

func (txn *Transaction) rowPrepare(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, row *map[string]interface{}) error {
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
		txn.log.Error(err, "failed schema validation of row", "row")
		err = errors.New(E_CONSTRAINT_VIOLATION)
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
	} else {
		uuid, err = txn.getGenerateUUID(ovsOp)
		if err != nil {
			return
		}
	}
	ovsResult.InitUUID(uuid)

	key := common.NewDataKey(txn.request.DBName, *ovsOp.Table, uuid)
	row := &map[string]interface{}{}

	*row = *ovsOp.Row
	txn.schema.Default(*ovsOp.Table, row)

	err = txn.rowPrepare(tableSchema, txn.mapUUID, ovsOp.Row)
	if err != nil {
		txn.log.Error(err, "failed to prepare row", "row", row, "table", ovsOp.Table)
		err = errors.New(E_CONSTRAINT_VIOLATION)
		return
	}
	setRowUUID(row, uuid)
	setRowVersion(row)
	k := key.String()
	val, e := makeValue(row)
	if e != nil {
		err = e
		return
	}
	table := txn.localCache.getLocalTableCache(*ovsOp.Table)
	table[k] = *row
	cmp := clientv3.Compare(clientv3.Version(k), "=", 0)
	txn.etcdTrx.appendIf(cmp)
	etcdOp := clientv3.OpPut(k, val)
	txn.etcdTrx.appendThen(etcdOp)
	tCache := txn.cache.getTable(*ovsOp.Table)
	for cName, destTable := range tCache.refColumns {
		cVal := (*row)[cName]
		if cVal == nil {
			continue
		}
		cSchema, _ := tableSchema.LookupColumn(cName)
		txn.refCounter.updateCounters(destTable, checkCounters(cVal, nil, cSchema.Type))
	}
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
	conditions, e := txn.updateConditions(ovsOp)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}

	rowProcess := func(key string, row map[string]interface{}, version int64) {
		var newRow *map[string]interface{}
		if ovsOp.Op == libovsdb.OperationUpdate {
			err = txn.rowPrepare(tableSchema, txn.mapUUID, ovsOp.Row)
			if err != nil {
				txn.log.Error(err, "failed to prepare row", "row", ovsOp.Row, "table", "table", ovsOp.Table)
				return
			}
			newRow, err = txn.rowUpdate(tableSchema, txn.mapUUID, &row, ovsOp.Row)
			if err != nil {
				txn.log.Error(err, "failed to update row", "row", ovsOp.Row)
				return
			}
		} else if ovsOp.Op == libovsdb.OperationMutate {
			newRow, err = txn.rowMutate(tableSchema, txn.mapUUID, &row, ovsOp.Mutations)
			if err != nil {
				txn.log.Error(err, "failed to row mutate", "row", row, "mutations", ovsOp.Mutations)
				return
			}
		}
		setRowVersion(newRow)
		err = txn.etcdModifyRow([]byte(key), newRow, version)
		if err != nil {
			return
		}
		for cName, destTable := range tCache.refColumns {
			newVal := (*newRow)[cName]
			oldVal := row[cName]
			cSchema, _ := tableSchema.LookupColumn(cName)
			txn.refCounter.updateCounters(destTable, checkCounters(newVal, oldVal, cSchema.Type))
		}
		ovsResult.IncrementCount()
	}

	uuid, e := conditions.getUUIDIfSelected()
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
		ok, err = conditions.isRowSelected(&row)
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		rowProcess(key, row, -1)
	}

	if uuid != "" {
		row, ok := tCache.rows[uuid]
		tCache = tCache.newEmptyTableCache()
		if ok {
			tCache.rows[uuid] = row
		}
	}
	for _, cRow := range tCache.rows {
		_, ok := lCache[cRow.key]
		if ok {
			// the row was modified by this transaction, we handled it above
			continue
		}
		if uuid == "" || len(conditions) > 1 {
			// there are other conditions in addition or instead of _uuid
			ok, err = conditions.isRowSelected(&cRow.row.Fields)
			if err != nil {
				return
			}
			if !ok {
				continue
			}
		}
		rowProcess(cRow.key, cRow.row.Fields, cRow.version)
	}
	return
}

/* Select and Delete */
func (txn *Transaction) doSelectDelete(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (err error, details string) {
	tCache := txn.cache.getTable(*ovsOp.Table)
	conditions, e := txn.updateConditions(ovsOp)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}
	uuid, e := conditions.getUUIDIfSelected()
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}

	if uuid != "" {
		kv, ok := tCache.rows[uuid]
		if !ok {
			return
		}
		tCache = &tableCache{rows: map[string]cachedRow{uuid: kv}}
	}
	for _, cRow := range tCache.rows {
		if uuid == "" || len(conditions) > 1 {
			// there are other conditions in addition or instead of _uuid
			ok, e := conditions.isRowSelected(&cRow.row.Fields)
			if e != nil {
				err = errors.New(E_INTERNAL_ERROR)
				return
			}
			if !ok {
				continue
			}
		}
		if ovsOp.Op == libovsdb.OperationDelete {
			etcdOp := clientv3.OpDelete(string(cRow.key))
			txn.etcdTrx.appendThen(etcdOp)
			tableSchema, e := txn.schema.LookupTable(*ovsOp.Table)
			if e != nil {
				err = errors.New(E_INTERNAL_ERROR)
				return
			}
			for cName, destTable := range tCache.refColumns {
				val := cRow.row.Fields[cName]
				if val == nil {
					continue
				}
				cSchema, _ := tableSchema.LookupColumn(cName)
				txn.refCounter.updateCounters(destTable, checkCounters(nil, val, cSchema.Type))
			}
			ovsResult.IncrementCount()
		} else if ovsOp.Op == libovsdb.OperationSelect {
			resultRow, e := reduceRowByColumns(&cRow.row.Fields, ovsOp.Columns)
			if e != nil {
				txn.log.Error(err, "failed to reduce row by columns", "columns", ovsOp.Columns)
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
	conditions, e := txn.updateConditions(ovsOp)
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}
	uuid, e := conditions.getUUIDIfSelected()
	if e != nil {
		err = errors.New(E_INTERNAL_ERROR)
		return
	}

	if uuid != "" {
		kv, ok := tCache.rows[uuid]
		if !ok {
			return
		}
		tCache = &tableCache{rows: map[string]cachedRow{uuid: kv}}
	}
	for _, kv := range tCache.rows {

		if uuid == "" || len(conditions) > 1 {
			// there are other conditions in addition or instead of _uuid
			ok, e := conditions.isRowSelected(&kv.row.Fields)
			if e != nil {
				err = errors.New(E_INTERNAL_ERROR)
				return
			}
			if !ok {
				continue
			}
		}
		if ovsOp.Columns != nil {
			ac, err1 := reduceRowByColumns(&kv.row.Fields, ovsOp.Columns)
			if err1 != nil {
				err = err1
				txn.log.Error(err, "failed to reduce row by columns", "row", kv.row.Fields, "columns", ovsOp.Columns)
				return
			}
			kv.row.Fields = *ac
		}

		for _, expected := range *ovsOp.Rows {
			err = txn.rowPrepare(tableSchema, txn.mapUUID, &expected)
			if err != nil {
				txn.log.Error(err, "failed to prepare row", "row", expected, "table", "table", ovsOp.Table)
				return
			}
			cond, e := txn.isEqualRow(tableSchema, &expected, &kv.row.Fields)
			txn.log.V(5).Info("checking row equal", "expected", expected, "actual", kv.row.Fields,
				"result", cond, "expected type", fmt.Sprintf("%T", expected), "actual type", fmt.Sprintf("%T", kv.row.Fields))
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
	key := common.NewCommentKey(timestamp)
	comment := *ovsOp.Comment
	etcdOp := clientv3.OpPut(key.String(), comment)
	txn.etcdTrx.appendThen(etcdOp)
	return nil, ""
}

/* assert */
func (txn *Transaction) doAssert(ovsOp *libovsdb.Operation, ovsResult *libovsdb.OperationResult) (error, string) {
	return nil, ""
}
