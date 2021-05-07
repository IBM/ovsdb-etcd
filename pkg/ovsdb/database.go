package ovsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
)

type Databaser interface {
	GetLock(ctx context.Context, id string) (Locker, error)
	AddMonitors(dbName string, updaters Key2Updaters, handler handlerKey)
	RemoveMonitors(dbName string, updaters map[string][]string, handler handlerKey)
	RemoveMonitor(dbName string)
	AddSchema(schemaName, schemaFile string) error
	GetSchemaFiles() []string
	GetData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error)
	PutData(ctx context.Context, key common.Key, obj interface{}) error
	GetSchema(name string) (string, bool)
}

type DatabaseEtcd struct {
	cli *clientv3.Client
	// dataBaseName -> schema
	Schemas     map[string]string
	SchemaFiles []string
	mu          sync.Mutex
	// databaseName -> monitor
	// We have a single monitor (etcd watcher) per database
	monitors map[string]*monitor
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

var EtcdClientTimeout = 100 * time.Millisecond

func NewDatabaseEtcd(cli *clientv3.Client) (Databaser, error) {
	return &DatabaseEtcd{cli: cli,
		Schemas: map[string]string{}, monitors: map[string]*monitor{}}, nil
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

func (con *DatabaseEtcd) AddSchema(schemaName, schemaFile string) error {
	data, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	json.Compact(buffer, data)
	con.Schemas[schemaName] = buffer.String()
	con.SchemaFiles = append(con.SchemaFiles, schemaFile)
	return nil
}

func (con *DatabaseEtcd) GetSchemaFiles() []string {
	return con.SchemaFiles
}

func (con *DatabaseEtcd) GetData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error) {
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
		return nil, err
	}
	if klog.V(6).Enabled() {
		klog.Infof(" GetData type %T \n", resp.Kvs)
	}
	if klog.V(7).Enabled() {
		for k, v := range resp.Kvs {
			klog.V(7).Infof("GetData k %v, v %v\n", k, v)
		}
	}
	return resp, err
}

func (con *DatabaseEtcd) GetSchema(name string) (string, bool) {
	return con.Schemas[name], true
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

func (con *DatabaseEtcd) AddMonitors(dbName string, updaters Key2Updaters, handler handlerKey) {
	if len(updaters) == 0 {
		// nothing to add
		return
	}
	con.mu.Lock()
	defer con.mu.Unlock()
	m, ok := con.monitors[dbName]
	if !ok {
		// we don't have monitors for this database yet, create a new one.
		m = newMonitor(dbName, con)
		ctxt, cancel := context.WithCancel(context.Background())
		m.cancel = cancel
		key := common.NewDBPrefixKey(dbName)
		wch := con.cli.Watch(clientv3.WithRequireLeader(ctxt), key.String(),
			clientv3.WithPrefix(),
			clientv3.WithCreatedNotify(),
			clientv3.WithPrevKV())
		m.watchChannel = wch
		con.monitors[dbName] = m
	}
	m.addUpdaters(updaters, handler)
	if !ok {
		m.start()
	}
}

func (con *DatabaseEtcd) RemoveMonitors(dbName string, updaters map[string][]string, handler handlerKey) {
	con.mu.Lock()
	defer con.mu.Unlock()
	m, ok := con.monitors[dbName]
	if !ok {
		klog.Warningf("Remove nonexistent db monitor %s", dbName)
		return
	}
	m.removeUpdaters(updaters, handler)
	if !m.hasHandlers() {
		m.cancel()
		delete(con.monitors, dbName)
	}
}

func (con *DatabaseEtcd) RemoveMonitor(dbName string) {
	con.mu.Lock()
	defer con.mu.Unlock()
	delete(con.monitors, dbName)
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

func (con *DatabaseMock) AddSchema(schemaName, schemaFile string) error {
	return con.Error
}

func (con *DatabaseMock) GetSchemaFiles() []string {
	return []string{}
}

func (con *DatabaseMock) GetData(key common.Key, keysOnly bool) (*clientv3.GetResponse, error) {
	return con.Response.(*clientv3.GetResponse), con.Error
}

func (con *DatabaseMock) PutData(ctx context.Context, key common.Key, obj interface{}) error {
	return con.Error
}

func (con *DatabaseMock) GetSchema(name string) (string, bool) {
	return con.Response.(string), con.Ok
}

func (con *DatabaseMock) GetUUID() string {
	return con.Response.(string)
}

func (con *DatabaseMock) AddMonitors(dbName string, updaters Key2Updaters, handler handlerKey) {
}

func (con *DatabaseMock) RemoveMonitors(dbName string, updaters map[string][]string, handler handlerKey) {
}

func (con *DatabaseMock) RemoveMonitor(dbName string) {
}
