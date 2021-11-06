package ovsdb

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

type cachedRow struct {
	row libovsdb.Row
	// etcd version of the row data
	version int64
	key     string
	// number of references to the row, relevant to rows in none root tables.
	counter int
}

type cache map[string]*databaseCache

type databaseCache struct {
	mu      sync.RWMutex
	dbCache map[string]tableCache
	log     logr.Logger
}

func newDatabaseCache(dSchema *libovsdb.DatabaseSchema, log logr.Logger) *databaseCache {
	dCache := databaseCache{dbCache: map[string]tableCache{}, log: log}
	for tName := range dSchema.Tables {
		dCache.dbCache[tName] = newTableCache(tName, dSchema)
	}
	return &dCache
}

// stores row indexed by uuid, except _Server.Database entries, which are indexed by database name.
type tableCache struct {
	rows    map[string]cachedRow
	tSchema *libovsdb.TableSchema
	// ref from this table columns to none root tables
	refColumns map[string]string
	// TODO add secondary index
}

func newTableCache(tableName string, dSchema *libovsdb.DatabaseSchema) tableCache {
	rColumns := map[string]string{}
	tSchema, ok := dSchema.Tables[tableName]
	if !ok {
		panic("cannot find table schema for " + tableName)
	}

	for cn, colSchema := range tSchema.Columns {
		if colSchema.TypeObj != nil {
			// the same column cannot contain references as key and as value
			if colSchema.TypeObj.Key != nil && colSchema.TypeObj.Key.RefTable != "" {
				refTable := colSchema.TypeObj.Key.RefTable
				if !dSchema.Tables[refTable].IsRoot {
					// we are aware about references to none root tables only
					rColumns[cn] = refTable
				}
			}
			if colSchema.TypeObj.Value != nil && colSchema.TypeObj.Value.RefTable != "" {
				refTable := colSchema.TypeObj.Value.RefTable
				if !dSchema.Tables[refTable].IsRoot {
					// we are aware about references to none root tables only
					rColumns[cn] = refTable
				}
			}
		}
	}
	return tableCache{rows: map[string]cachedRow{}, refColumns: rColumns, tSchema: &tSchema}
}

func (tc *tableCache) newEmptyTableCache() *tableCache {
	return &tableCache{rows: map[string]cachedRow{}, refColumns: tc.refColumns}
}

func (tc *tableCache) size() int {
	return len(tc.rows)
}

func (c *cache) addDatabaseCache(dbSchema *libovsdb.DatabaseSchema, etcdClient *clientv3.Client, log logr.Logger) (int64, error) {
	dbName := dbSchema.Name
	if _, ok := (*c)[dbName]; ok {
		return -1, errors.New("Duplicate DatabaseCache: " + dbName)
	}
	dbCache := newDatabaseCache(dbSchema, log)
	(*c)[dbName] = dbCache
	// we don't need etcd watcher
	if etcdClient == nil {
		return 0, nil
	}
	ctxt := context.Background()
	key := common.NewDBPrefixKey(dbName)
	resp, err := etcdClient.Get(ctxt, key.String(), clientv3.WithPrefix())
	if err != nil {
		log.Error(err, "get KeyData")
		return -1, err
	}
	err = dbCache.storeValues(resp.Kvs)
	if err != nil {
		log.Error(err, "storeValues")
		return -1, err
	}
	return resp.Header.Revision, nil
}

func newCachedRow(key string, value []byte, version int64) (*cachedRow, error) {
	row := libovsdb.Row{}
	err := row.UnmarshalJSON(value)
	if err != nil {
		return nil, errors.WithMessagef(err, "unmarshal to row, key %s", key)
	}
	return &cachedRow{row: row, key: key, version: version}, nil
}

func (dc *databaseCache) getRow(key common.Key) (cachedRow, bool) {
	tCache := dc.getTable(key.TableName)
	row, ok := tCache.rows[key.UUID]
	return row, ok
}

// TODO combine with updateCache
func (dc *databaseCache) storeValues(kvs []*mvccpb.KeyValue) error {
	rows := map[common.Key]*cachedRow{}
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		key, err := common.ParseKey(string(kv.Key))
		if err != nil {
			return err
		}
		if key.IsCommentKey() {
			continue
		}

		cr, err := newCachedRow(key.String(), kv.Value, kv.Version)
		if err != nil {
			dc.log.Error(err, "cannot store value in the cache")
			return err
		}
		rows[*key] = cr
	}
	// now actually update the cache
	dc.updateRows(rows)
	return nil
}

func (dc *databaseCache) updateCountersSet(newVal interface{}, oldVal interface{}, refTable *tableCache, tableKey *common.Key, newRows map[common.Key]*cachedRow) {
	newValSet := interfaceToSet(newVal)
	oldValSet := interfaceToSet(oldVal)
	for _, uuid := range newValSet.GoSet {
		if !oldValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter++
				refTable.rows[ovsUUID.GoUUID] = cRow
			} else if tableKey != nil {
				// we update counters only for new rows
				key := common.NewDataKey(tableKey.DBName, tableKey.TableName, ovsUUID.GoUUID)
				newRow, ok := newRows[key]
				if ok {
					newRow.counter++
				}
			}
		}
	}
	for _, uuid := range oldValSet.GoSet {
		if !newValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter--
				refTable.rows[ovsUUID.GoUUID] = cRow
			}
		}
	}
}

func checkCountersUUID(newVal interface{}, oldVal interface{}) map[string]int {
	counters := map[string]int{}
	if newVal == nil {
		// oldValue and newValue cannot be both nil, we checked it before
		ovsUUID := oldVal.(libovsdb.UUID)
		if ovsUUID.GoUUID != libovsdb.DefaultUUID.GoUUID {
			counters[ovsUUID.GoUUID]--
			return counters
		}
		return nil

	}
	if oldVal == nil {
		ovsUUID := newVal.(libovsdb.UUID)
		if ovsUUID.GoUUID != libovsdb.DefaultUUID.GoUUID {
			counters[ovsUUID.GoUUID]++
			return counters
		}
		return nil
	}
	newUUID := newVal.(libovsdb.UUID)
	oldUUID := oldVal.(libovsdb.UUID)
	if newUUID.GoUUID == oldUUID.GoUUID {
		return nil
	}
	counters[newUUID.GoUUID]++
	counters[oldUUID.GoUUID]--
	return counters
}

func checkCountersSet(newVal interface{}, oldVal interface{}) map[string]int {
	newValSet := interfaceToSet(newVal)
	oldValSet := interfaceToSet(oldVal)
	counters := map[string]int{}
	for _, uuid := range newValSet.GoSet {
		if !oldValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]++
		}
	}
	for _, uuid := range oldValSet.GoSet {
		if !newValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]--
		}
	}
	return counters
}

func (dc *databaseCache) updateCountersUUID(newVal interface{}, oldVal interface{}, refTable *tableCache, tableKey *common.Key, newRows map[common.Key]*cachedRow) {
	if newVal != nil {
		newUUID := newVal.(libovsdb.UUID)
		if oldVal != nil {
			oldUUID := oldVal.(libovsdb.UUID)
			if newUUID.GoUUID == oldUUID.GoUUID {
				return
			}
		}
		cRow, ok := refTable.rows[newUUID.GoUUID]
		if ok {
			cRow.counter++
			refTable.rows[newUUID.GoUUID] = cRow
		} else if tableKey != nil {
			// the referenced row is in the events list.
			key := common.NewDataKey(tableKey.DBName, tableKey.TableName, newUUID.GoUUID)
			newRow, ok := newRows[key]
			if ok {
				newRow.counter++
			}
		}
	}
	if oldVal != nil {
		// the case that oldVal == newVal we checked before
		oldUUID := oldVal.(libovsdb.UUID)
		cRow, ok := refTable.rows[oldUUID.GoUUID]
		if ok {
			cRow.counter--
			refTable.rows[oldUUID.GoUUID] = cRow
		}
	}
}

func (dc *databaseCache) updateCountersMap(newVal interface{}, oldVal interface{}, refTable *tableCache, tableKey *common.Key, newRows map[common.Key]*cachedRow) {
	var newValMap libovsdb.OvsMap
	var oldValMap libovsdb.OvsMap
	if newVal == nil {
		newValMap = libovsdb.OvsMap{}
	} else {
		newValMap = newVal.(libovsdb.OvsMap)
	}
	if oldVal == nil {
		oldValMap = libovsdb.OvsMap{}
	} else {
		oldValMap = oldVal.(libovsdb.OvsMap)
	}
	for k, newV := range newValMap.GoMap {
		oldV, ok := oldValMap.GoMap[k]
		if !ok {
			// new reference
			ovsUUID := newV.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter++
				refTable.rows[ovsUUID.GoUUID] = cRow
			} else if tableKey != nil {
				// the referenced row is in the events list.
				key := common.NewDataKey(tableKey.DBName, tableKey.TableName, ovsUUID.GoUUID)
				newRow, ok := newRows[key]
				if ok {
					newRow.counter++
				}
			}
		} else {
			if !reflect.DeepEqual(oldV, newV) {
				ovsUUID := newV.(libovsdb.UUID)
				cRow, ok := refTable.rows[ovsUUID.GoUUID]
				if ok {
					cRow.counter++
					refTable.rows[ovsUUID.GoUUID] = cRow
				}
				ovsUUID = oldV.(libovsdb.UUID)
				cRow, ok = refTable.rows[ovsUUID.GoUUID]
				if ok {
					cRow.counter--
					refTable.rows[ovsUUID.GoUUID] = cRow
				}
			}
		}
	}
	for k, oldV := range oldValMap.GoMap {
		_, ok := newValMap.GoMap[k]
		if !ok {
			ovsUUID := oldV.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter--
				refTable.rows[ovsUUID.GoUUID] = cRow
			}
		}
	}
}

func checkCountersMap(newVal interface{}, oldVal interface{}) map[string]int {
	var newValMap libovsdb.OvsMap
	var oldValMap libovsdb.OvsMap
	var ok bool
	if newVal == nil {
		newValMap = libovsdb.OvsMap{}
	} else {
		newValMap, ok = newVal.(libovsdb.OvsMap)
		if !ok {
			// TODO
		}
	}
	if oldVal == nil {
		oldValMap = libovsdb.OvsMap{}
	} else {
		oldValMap, ok = oldVal.(libovsdb.OvsMap)
		if !ok {
			// TODO
		}
	}
	counters := map[string]int{}
	for k, newV := range newValMap.GoMap {
		oldV, ok := oldValMap.GoMap[k]
		if !ok {
			ovsUUID := newV.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]++
		} else {
			if !reflect.DeepEqual(oldV, newV) {
				ovsUUID := newV.(libovsdb.UUID)
				counters[ovsUUID.GoUUID]++
				ovsUUID = oldV.(libovsdb.UUID)
				counters[ovsUUID.GoUUID]--
			}
		}
	}
	for k, oldV := range oldValMap.GoMap {
		_, ok := newValMap.GoMap[k]
		if !ok {
			ovsUUID := oldV.(libovsdb.UUID)
			ovsUUID = oldV.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]--
		}
	}
	return counters
}

func (dc *databaseCache) updateCounters(newVal, oldVal interface{}, refTable *tableCache, columnType string, tableKey *common.Key, newRows map[common.Key]*cachedRow) {
	if newVal == nil && oldVal == nil {
		return
	}
	switch columnType {
	case libovsdb.TypeSet:
		dc.updateCountersSet(newVal, oldVal, refTable, tableKey, newRows)
	case libovsdb.TypeMap:
		dc.updateCountersMap(newVal, oldVal, refTable, tableKey, newRows)
	case libovsdb.TypeUUID:
		dc.updateCountersUUID(newVal, oldVal, refTable, tableKey, newRows)
	default:
		//TODO we cannot be here add error
	}
}

func checkCounters(newVal interface{}, oldVal interface{}, columnType string) map[string]int {
	if newVal == nil && oldVal == nil {
		return nil
	}
	switch columnType {
	case libovsdb.TypeSet:
		return checkCountersSet(newVal, oldVal)
	case libovsdb.TypeMap:
		return checkCountersMap(newVal, oldVal)
	case libovsdb.TypeUUID:
		return checkCountersUUID(newVal, oldVal)
	default:
		//TODO we cannot be here add error
	}
	return nil
}

func (dc *databaseCache) updateRows(newRows map[common.Key]*cachedRow) {
	// now update the cache
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for key, row := range newRows {
		tb := dc.getTable(key.TableName)
		if row == nil {
			// delete event
			oldRow, ok := tb.rows[key.UUID]
			if !ok {
				continue
			}
			for cName, destTable := range tb.refColumns {
				val := oldRow.row.Fields[cName]
				if val == nil {
					continue
				}
				dTable := dc.getTable(destTable)
				cSchema, _ := tb.tSchema.LookupColumn(cName)
				dc.updateCounters(nil, val, dTable, cSchema.Type, nil, nil)
			}
			delete(tb.rows, key.UUID)
		} else {
			oldRow, ok := tb.rows[key.UUID]
			for cName, destTable := range tb.refColumns {
				newVal := row.row.Fields[cName]
				if newVal == nil && !ok {
					// the reference was not set and is not setting
					continue
				}
				dTable := dc.getTable(destTable)
				cSchema, _ := tb.tSchema.LookupColumn(cName)
				dTablekey := common.NewTableKey(key.DBName, destTable)
				if ok {
					dc.updateCounters(newVal, oldRow.row.Fields[cName], dTable, cSchema.Type, &dTablekey, newRows)
				} else {
					dc.updateCounters(newVal, nil, dTable, cSchema.Type, &dTablekey, newRows)
				}
			}
			if ok {
				row.counter = oldRow.counter
			}
			tb.rows[key.UUID] = *row
		}
	}
}

// should be called under locked dc.mu
func (dc *databaseCache) getTable(tableName string) *tableCache {
	tb, ok := dc.dbCache[tableName]
	if !ok {
		panic(fmt.Sprintf("There is no table %s in the cache", tableName))
	}
	return &tb
}

func (dc *databaseCache) size() int {
	var ret int
	for _, tbCache := range dc.dbCache {
		ret += tbCache.size()
	}
	return ret
}

func (c *cache) size() int {
	var ret int
	for _, dbCache := range *c {
		ret += dbCache.size()
	}
	return ret
}

func (c *cache) getDBCache(dbname string) *databaseCache {
	db, ok := (*c)[dbname]
	if !ok {
		db = &databaseCache{dbCache: map[string]tableCache{}}
		(*c)[dbname] = db
	}
	return db
}

func interfaceToSet(val interface{}) libovsdb.OvsSet {
	if val == nil {
		return libovsdb.OvsSet{}
	}
	switch val.(type) {
	case libovsdb.OvsSet:
		return val.(libovsdb.OvsSet)
	case libovsdb.UUID:
		return libovsdb.OvsSet{GoSet: []interface{}{val.(libovsdb.UUID)}}
	}
	panic(fmt.Errorf("wrong conversation type %T", val))
}
