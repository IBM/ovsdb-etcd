package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ibm/ovsdb-etcd/pkg/types/_Server"

	"github.com/go-logr/logr"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

type Databaser interface {
	GetLock(ctx context.Context, id string) (Locker, error)
	CreateMonitor(dbName string, handler *Handler, log logr.Logger) *dbMonitor
	AddSchema(schemaFile string) error
	GetSchemas() libovsdb.Schemas
	GetKeyData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error)
	GetData(keys []common.Key) (*clientv3.TxnResponse, error)
	PutData(ctx context.Context, key common.Key, obj interface{}) error
	GetSchema(name string) map[string]interface{}
	DbLock(dbName string)
	DbUnlock(dbName string)
}

type DatabaseEtcd struct {
	cli        *clientv3.Client
	Schemas    libovsdb.Schemas // dataBaseName -> schema
	strSchemas map[string]map[string]interface{}
	locks      map[string]*sync.Mutex
	mu         sync.Mutex
}

type Locker interface {
	tryLock() error
	lock() error
	unlock() error
	cancel()
}

type lock struct {
	mutex    *concurrency.Mutex
	myCancel context.CancelFunc
	cntx     context.Context
}

func (l *lock) tryLock() error {
	return l.mutex.TryLock(l.cntx)
}

func (l *lock) lock() error {
	return l.mutex.Lock(l.cntx)
}

func (l *lock) unlock() error {
	return l.mutex.Unlock(l.cntx)
}

func (l *lock) cancel() {
	l.myCancel()
}

var EtcdClientTimeout = time.Second

func NewEtcdClient(endpoints []string) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:          endpoints,
		DialTimeout:        30 * time.Second,
		MaxCallSendMsgSize: 120 * 1024 * 1024,
		MaxCallRecvMsgSize: 0, /* max */
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func NewDatabaseEtcd(cli *clientv3.Client) (Databaser, error) {
	return &DatabaseEtcd{cli: cli,
		Schemas: libovsdb.Schemas{}, strSchemas: map[string]map[string]interface{}{}, locks: map[string]*sync.Mutex{}}, nil
}

func (con *DatabaseEtcd) DbLock(dbName string) {
	con.locks[dbName].Lock()
}

func (con *DatabaseEtcd) DbUnlock(dbName string) {
	con.locks[dbName].Unlock()
}

func (con *DatabaseEtcd) GetLock(ctx context.Context, id string) (Locker, error) {
	ctctx, cancel := context.WithCancel(ctx)
	session, err := concurrency.NewSession(con.cli, concurrency.WithContext(ctctx))
	if err != nil {
		cancel()
		return nil, err
	}
	key := common.NewLockKey(id)
	mutex := concurrency.NewMutex(session, key.String())
	return &lock{mutex: mutex, myCancel: cancel, cntx: ctctx}, nil
}

func (con *DatabaseEtcd) AddSchema(schemaFile string) error {
	data, err := common.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	err = con.Schemas.AddFromBytes(data)
	if err != nil {
		return err
	}
	schemaMap := map[string]interface{}{}
	err = json.Unmarshal(data, &schemaMap)
	if err != nil {
		return err
	}
	schemaName := schemaMap["name"].(string)
	con.mu.Lock()
	con.strSchemas[schemaName] = schemaMap
	con.locks[schemaName] = &sync.Mutex{}
	con.mu.Unlock()
	schemaSet, err := libovsdb.NewOvsSet(string(data))
	srv := _Server.Database{Model: "standalone", Name: schemaName, Uuid: libovsdb.UUID{GoUUID: uuid.NewString()},
		Connected: true, Leader: true, Schema: *schemaSet, Version: libovsdb.UUID{GoUUID: uuid.NewString()}}
	key := common.NewDataKey("_Server", "Database", schemaName)
	ctx, cancel := context.WithTimeout(context.Background(), EtcdClientTimeout)
	defer cancel()
	if err := (*con).PutData(ctx, key, srv); err != nil {
		return err
	}
	return nil
}

func (con *DatabaseEtcd) GetSchemas() libovsdb.Schemas {
	return con.Schemas
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
		klog.Errorf("GetKeyData: %s", err)
		return nil, err
	}
	if klog.V(8).Enabled() {
		for k, v := range resp.Kvs {
			klog.V(8).Infof("GetKeyData k %v, v %v\n", k, v)
		}
	}
	return resp, err
}

func (con *DatabaseEtcd) GetData(keys []common.Key) (*clientv3.TxnResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdClientTimeout)
	ops := []clientv3.Op{}
	for _, key := range keys {
		ops = append(ops, clientv3.OpGet(key.String(), clientv3.WithPrefix()))
	}
	res, err := con.cli.Txn(ctx).Then(ops...).Commit()
	cancel()
	if err != nil {
		klog.Errorf("GetData returned error: %v", err)
	} else {
		klog.Infof("GetData succeeded %v revision %d", res.Succeeded, res.Header.Revision)
	}
	return res, err
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

type KeyValue struct {
	Key   common.Key
	Value map[string]interface{}
}

func NewKeyValue(etcdKV *mvccpb.KeyValue) (*KeyValue, error) {
	if etcdKV == nil {
		return nil, fmt.Errorf("nil etcdKV")
	}
	kv := new(KeyValue)

	/* key */
	if etcdKV.Key == nil {
		return nil, fmt.Errorf("nil key")
	}
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

type DatabaseMock struct {
	Response interface{}
	Error    error
	Ok       bool
	mu       sync.Mutex
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
	return &DatabaseMock{}, nil
}

func (con *DatabaseMock) GetLock(ctx context.Context, id string) (Locker, error) {

	return &LockerMock{}, nil
}

func (con *DatabaseMock) AddSchema(schemaFile string) error {
	return con.Error
}

func (con *DatabaseMock) GetSchemas() libovsdb.Schemas {
	return con.Response.(libovsdb.Schemas)
}

func (con *DatabaseMock) GetKeyData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error) {
	return con.Response.(*clientv3.GetResponse), con.Error
}

func (con *DatabaseMock) GetData(keys []common.Key) (*clientv3.TxnResponse, error) {
	return con.Response.(*clientv3.TxnResponse), con.Error
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

func (con *DatabaseMock) DbLock(dbName string)   {}
func (con *DatabaseMock) DbUnlock(dbName string) {}
