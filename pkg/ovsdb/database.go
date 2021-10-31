package ovsdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

type Databaser interface {
	CreateMonitor(dbName string, handler *Handler, log logr.Logger) *dbMonitor
	AddSchema(schemaFile string) error
	GetKeyData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error)
	PutData(ctx context.Context, key common.Key, obj interface{}) error
	GetSchema(dbName string) map[string]interface{}
	GetDBSchema(dbName string) (*libovsdb.DatabaseSchema, bool)
	GetDBCache(dbName string) (*databaseCache, error)
	GetServerID() string
	StartLeaderElection()
}

const (
	ModClustered  = "clustered"
	ModStandalone = "standalone"

	DBColName      = "name"
	DBColModel     = "model"
	DBColConnected = "connected"
	DBColLeader    = "leader"
	DBColSchema    = "schema"
	DBColCID       = "cid"
	DBColSID       = "sid"
	DBColIndex     = "index"
)

type DatabaseEtcd struct {
	cli *clientv3.Client
	log logr.Logger
	//  we don't protect the fields below, because they are initialized during start of the server and aren't modified after that
	cache      cache
	schemas    libovsdb.Schemas // dataBaseName -> schema
	strSchemas map[string]map[string]interface{}
	serverID   string
	model      string
	dbName     string
}

var EtcdClientTimeout = time.Second

func NewEtcdClient(ctx context.Context, endpoints []string, keepAliveTime, keepAliveTimeout time.Duration) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		Endpoints:          endpoints,
		Context:            ctx,
		DialTimeout:        30 * time.Second,
		MaxCallSendMsgSize: 120 * 1024 * 1024,
		MaxCallRecvMsgSize: 0, /* max */
	}
	if keepAliveTime > 0 {
		cfg.DialKeepAliveTime = keepAliveTime
	}
	if keepAliveTimeout > 0 {
		cfg.DialKeepAliveTimeout = keepAliveTimeout
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func NewDatabaseEtcd(cli *clientv3.Client, model string, log logr.Logger) (Databaser, error) {
	if model != ModClustered && model != ModStandalone {
		return nil, errors.New(fmt.Sprintf("wrong deployment model %s", model))
	}
	return &DatabaseEtcd{cli: cli, log: log, cache: cache{}, model: model,
		schemas: libovsdb.Schemas{}, strSchemas: map[string]map[string]interface{}{}, serverID: uuid.NewString()}, nil
}

func (con *DatabaseEtcd) AddSchema(schemaFile string) error {
	data, err := common.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	err = con.schemas.AddFromBytes(data)
	if err != nil {
		return err
	}

	// we need this map[string]interface{} schema representation to correctly serve get get-schema calls.
	// TODO check possibility to combine with the schemas objects.
	schemaMap := map[string]interface{}{}
	err = json.Unmarshal(data, &schemaMap)
	if err != nil {
		return err
	}
	schemaName := schemaMap["name"].(string)
	con.strSchemas[schemaName] = schemaMap

	schemaSet, err := libovsdb.NewOvsSet(string(data))
	if err != nil {
		return err
	}
	dbsSchema, err := con.schemas.LookupTable(IntServer, IntDatabase)
	if err != nil {
		return err
	}
	row := make(map[string]interface{})
	dbsSchema.Default(&row)
	row[DBColName] = schemaName
	row[DBColConnected] = true
	row[DBColSchema] = *schemaSet
	row[libovsdb.ColVersion] = libovsdb.UUID{GoUUID: uuid.NewString()}
	row[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: uuid.NewString()}
	if schemaName == IntServer {
		err = con.cache.addDatabaseCache(con.schemas[schemaName], nil, con.log)
		if err != nil {
			return err
		}
		row[DBColModel] = ModStandalone
		row[DBColLeader] = true
	} else {
		con.dbName = schemaName
		err = con.cache.addDatabaseCache(con.schemas[schemaName], con.cli, con.log)
		if err != nil {
			return err
		}
		row[DBColModel] = con.model
		if con.model == ModStandalone {
			row[DBColLeader] = true
		} else {
			sidSet, err := libovsdb.NewOvsSet(libovsdb.UUID{GoUUID: con.GetServerID()})
			if err != nil {
				return err
			}
			cid, err := con.getClusterID()
			if err != nil {
				return err
			}
			cidSet, err := libovsdb.NewOvsSet(libovsdb.UUID{GoUUID: cid})
			if err != nil {
				return err
			}
			row[DBColLeader] = true // TODO return false
			row[DBColCID] = cidSet
			row[DBColSID] = sidSet
		}
	}

	key := common.NewDataKey(IntServer, IntDatabase, schemaName)
	dbCache := con.cache.getDBCache(IntServer)
	val, err := json.Marshal(row)
	if err != nil {
		return err
	}
	kv := mvccpb.KeyValue{Key: []byte(key.String()), Value: val}
	dbCache.storeValues([]*mvccpb.KeyValue{&kv})

	return nil
}

func (con *DatabaseEtcd) StartLeaderElection() {
	if con.model == ModStandalone {
		// do nothing
		return
	}
	// create a sessions to elect a Leader
	s, err := concurrency.NewSession(con.cli, concurrency.WithTTL(5))
	if err != nil {
		con.log.Error(err, "new session returned")
	}
	//defer s.Close()
	election := concurrency.NewElection(s, common.NewTableKey(IntServer, IntLeaderElection).String())
	// TODO set context
	go func() {
		// Elect a leader (or wait that the leader resign)
		ctx := context.Background()
		for {

			err := election.Campaign(ctx, "e")
			if err != nil {
				con.log.Error(err, "Leader Election error", "serverId", con.serverID)
			} else {
				con.log.V(1).Info("err is nil")
				break
			}
		}
		servData := con.cache.getDBCache(IntServer)
		tCache := (*servData).getTable(IntDatabase)
		cRow, ok := tCache.rows[con.dbName]
		if !ok {
			panic(fmt.Sprintf("db entry %s doesn't exsist in _Server/Database", con.dbName))
		}
		cRow.row.Fields[DBColLeader] = true
		tCache.rows[con.dbName] = cRow
		con.log.V(1).Info("I'm the leader", "serverID", con.serverID)
		// print to pod log too
		fmt.Printf("I'm the leader, serverID %s\n", con.serverID)
	}()
}

func (con *DatabaseEtcd) getClusterID() (string, error) {
	cidKey := common.NewTableKey(IntServer, IntClusterID).String()
	cidTmpValue := uuid.NewString()
	resp, err := con.cli.Txn(context.Background()).If(clientv3.Compare(clientv3.CreateRevision(cidKey), "=", 0)).Then(clientv3.OpPut(cidKey, cidTmpValue)).Else(clientv3.OpGet(cidKey)).Commit()
	if err != nil {
		return "", err
	}
	if resp.Succeeded {
		return cidTmpValue, nil
	} else {
		return string(resp.Responses[0].GetResponseRange().Kvs[0].Value), nil
	}
}

func (con *DatabaseEtcd) GetDBSchema(dbName string) (*libovsdb.DatabaseSchema, bool) {
	s, ok := con.schemas[dbName]
	return s, ok
}

func (con *DatabaseEtcd) GetDBCache(dbName string) (*databaseCache, error) {
	if con.cache == nil {
		err := errors.New(ErrInternalError)
		con.log.V(1).Info("Cache is not created")
		return nil, err
	}
	dbCache, ok := con.cache[dbName]
	if !ok {
		err := errors.New(ErrInternalError)
		con.log.V(1).Info("Database cache is not created", "dbName", dbName)
		return nil, err
	}
	return dbCache, nil
}

func (con *DatabaseEtcd) GetKeyData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdClientTimeout)
	var resp *clientv3.GetResponse
	var err error
	if keysOnly {
		resp, err = con.cli.Get(ctx, key.String(), clientv3.WithPrefix(), clientv3.WithKeysOnly())
	} else {
		resp, err = con.cli.Get(ctx, key.String(), clientv3.WithPrefix())
	}
	cancel()
	if err != nil {
		con.log.Error(err, "GetKeyData")
		return nil, err
	}
	return resp, err
}

func (con *DatabaseEtcd) GetSchema(name string) map[string]interface{} {
	return con.strSchemas[name]
}

func (con *DatabaseEtcd) PutData(ctx context.Context, key common.Key, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = con.cli.Put(ctx, key.String(), string(data))
	if err != nil {
		return err
	}
	return nil
}

func (con *DatabaseEtcd) CreateMonitor(dbName string, handler *Handler, log logr.Logger) *dbMonitor {
	m := newMonitor(dbName, handler, log)
	ctxt, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	key := common.NewDBPrefixKey(dbName)
	wch := con.cli.Watch(clientv3.WithRequireLeader(ctxt), key.String(),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV())
	m.watchChannel = wch
	return m
}

func (con *DatabaseEtcd) GetServerID() string {
	return con.serverID
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

type DatabaseMock struct {
	Response interface{}
	Error    error
	Ok       bool
	mu       sync.Mutex
	ServerID string
}

type LockerMock struct {
	Mu    sync.Mutex
	Error error
}

func (l *LockerMock) tryLock() error {
	return l.Error
}

func (l *LockerMock) lock() error {
	l.Mu.Lock()
	return nil
}

func (l *LockerMock) unlock() error {
	l.Mu.Unlock()
	return nil
}

func (l *LockerMock) cancel() {
	l.Mu.Unlock()
}

func NewDatabaseMock() (Databaser, error) {
	return &DatabaseMock{ServerID: uuid.NewString()}, nil
}

func (con *DatabaseMock) StartLeaderElection() {
	return
}

func (con *DatabaseMock) AddSchema(schemaFile string) error {
	return con.Error
}

func (con *DatabaseMock) GetDBSchema(dbName string) (*libovsdb.DatabaseSchema, bool) {
	s, ok := con.Response.(libovsdb.Schemas)[dbName]
	return s, ok
}

func (con *DatabaseMock) GetKeyData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error) {
	return con.Response.(*clientv3.GetResponse), con.Error
}

func (con *DatabaseMock) PutData(ctx context.Context, key common.Key, obj interface{}) error {
	return con.Error
}

func (con *DatabaseMock) GetSchema(name string) map[string]interface{} {
	return nil
}

func (con *DatabaseMock) GetUUID() string {
	return con.Response.(string)
}

func (con *DatabaseMock) CreateMonitor(dbName string, handler *Handler, log logr.Logger) *dbMonitor {
	m := newMonitor(dbName, handler, log)
	_, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	return m
}

func (con *DatabaseMock) GetDBCache(dbName string) (*databaseCache, error) {
	// TODO
	return nil, nil
}

func (con *DatabaseMock) GetServerID() string {
	return con.ServerID
}
