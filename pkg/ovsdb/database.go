package ovsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

type Databaser interface {
	GetLock(ctx context.Context, id string) (Locker, error)
	AddSchema(schemaName, schemaFile string) error
	GetData(prefix string, keysOnly bool) (*clientv3.GetResponse, error)
	PutData(ctx context.Context, key string, obj interface{}) error
	GetMarshaled(prefix string, columns []interface{}) (*[]map[string]string, error)
	GetSchema(name string) (string, bool)
	AddMonitor(prefix string, mcr ovsjson.MonitorCondRequest, isV1 bool, hand *handlerKey)
	DelMonitor(prefix string, mcr ovsjson.MonitorCondRequest, isV1 bool, hand *handlerKey)
	Close()
}

type DatabaseEtcd struct {
	cli     *clientv3.Client
	Schemas map[string]string
	// map from prefix to monitors
	monitors map[string]monitor
	mu       sync.Mutex
	prefix   string
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

var EtcdDialTimeout = 5 * time.Second
var EtcdClientTimeout = 100 * time.Millisecond

func NewDatabaseEtcd(endpoints []string, prefix string) (Databaser, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: EtcdDialTimeout,
	})
	if err != nil {
		klog.Errorf("NewETCDConenctor , error: ", err)
		return nil, err
	}
	klog.Info("etcd client is connected")
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	return &DatabaseEtcd{cli: cli,
		Schemas: make(map[string]string), prefix: prefix}, nil
}

func (con *DatabaseEtcd) Close() {
	con.cli.Close()
}

func (con *DatabaseEtcd) GetLock(ctx context.Context, id string) (Locker, error) {
	ctctx, cancel := context.WithCancel(ctx)
	session, err := concurrency.NewSession(con.cli, concurrency.WithContext(ctctx))
	if err != nil {
		cancel()
		return nil, err
	}
	mutex := concurrency.NewMutex(session, con.prefix+"locks/"+id)
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
	return nil
}

func (con *DatabaseEtcd) GetData(keysPrefix string, keysOnly bool) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdClientTimeout)
	var resp *clientv3.GetResponse
	var err error
	if keysOnly {
		resp, err = con.cli.Get(ctx, con.prefix+keysPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	} else {
		resp, err = con.cli.Get(ctx, con.prefix+keysPrefix, clientv3.WithPrefix())
	}
	cancel()
	if err != nil {
		return nil, err
	}
	if klog.V(6).Enabled() {
		klog.Infof(" GetDatatype %T \n", resp.Kvs)
	}
	if klog.V(7).Enabled() {
		for k, v := range resp.Kvs {
			klog.V(7).Infof("GetData k %v, v %v\n", k, v)
		}
	}
	return resp, err
}

// TODO replace
func (con *DatabaseEtcd) GetMarshaled(keysPrefix string, columns []interface{}) (*[]map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdClientTimeout)
	resp, err := con.cli.Get(ctx, con.prefix+keysPrefix, clientv3.WithFromKey())
	cancel()
	if err != nil {
		return nil, err
	}
	retMaps := map[string]map[string]string{}
	columnsMap := map[string]bool{}
	for _, col := range columns {
		columnsMap[col.(string)] = true
	}
	for _, v := range resp.Kvs {
		keys := strings.Split(string(v.Key), "/")
		len := len(keys)
		if _, ok := columnsMap[keys[len-1]]; !ok {
			continue
		}
		valsmap, ok := retMaps[keys[len-3]]
		if !ok {
			valsmap = map[string]string{}
		}
		valsmap[keys[len-1]] = string(v.Value)

		// TODO
		retMaps[keys[len-3]] = valsmap
	}
	values := []map[string]string{}
	for _, value := range retMaps {
		values = append(values, value)
	}
	return &values, nil
}

func (con *DatabaseEtcd) GetSchema(name string) (string, bool) {
	return con.Schemas[name], true
}

func (con *DatabaseEtcd) PutData(ctx context.Context, key string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = con.cli.Put(ctx, con.prefix+key, string(data))
	if err != nil {
		return err
	}
	return nil
}
func (con *DatabaseEtcd) AddMonitor(keysPrefix string, mcr ovsjson.MonitorCondRequest, isV1 bool, hand *handlerKey) {
	con.mu.Lock()
	defer con.mu.Unlock()
	if monitor, ok := con.monitors[keysPrefix]; !ok {
		con.monitors[keysPrefix] = *newMonitor(con.cli, con.prefix+keysPrefix, mcr, isV1, hand)
	} else {
		monitor.addHandler(mcr, isV1, hand)
	}
}

func (con *DatabaseEtcd) DelMonitor(keysPrefix string, mcr ovsjson.MonitorCondRequest, isV1 bool, hand *handlerKey) {
	con.mu.Lock()
	defer con.mu.Unlock()
	if monitor, ok := con.monitors[keysPrefix]; !ok {
		klog.Warningf("Delete unexisting monitor from %s", keysPrefix)
	} else {
		if monitor.delHandler(mcr, isV1, hand) {
			delete(con.monitors, keysPrefix)
		}
	}
}

type DatabaseMock struct {
	Response interface{}
	Error    error
	Ok       bool
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

func (con *DatabaseMock) Close() {
}

func (con *DatabaseMock) GetLock(ctx context.Context, id string) (Locker, error) {

	return &LockerMock{}, nil
}

func (con *DatabaseMock) AddSchema(schemaName, schemaFile string) error {
	return con.Error
}

func (con *DatabaseMock) GetData(keysPrefix string, keysOnly bool) (*clientv3.GetResponse, error) {
	return con.Response.(*clientv3.GetResponse), con.Error
}

func (con *DatabaseMock) PutData(ctx context.Context, key string, obj interface{}) error {
	return con.Error
}

func (con *DatabaseMock) GetMarshaled(keysPrefix string, columns []interface{}) (*[]map[string]string, error) {
	return con.Response.(*[]map[string]string), con.Error
}

func (con *DatabaseMock) GetSchema(name string) (string, bool) {
	return con.Response.(string), con.Ok
}

func (con *DatabaseMock) GetUUID() string {
	return con.Response.(string)
}

func (con *DatabaseMock) AddMonitor(keysPrefix string, mcr ovsjson.MonitorCondRequest, isV1 bool, hand *handlerKey) {
}

func (con *DatabaseMock) DelMonitor(keysPrefix string, mcr ovsjson.MonitorCondRequest, isV1 bool, hand *handlerKey) {
}
