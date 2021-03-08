package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/creachadair/jrpc2"
	ovsdbjson "github.com/ebay/libovsdb"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/types/_Server"
)

type DBServer struct {
	cli         *clientv3.Client
	uuid        string
	schemas     map[string]string
	schemaTypes map[string]map[string]map[string]string
}

func NewDBServer(endpoints []string) (*DBServer, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		klog.Errorf("NewETCDConenctor , error: ", err)
		return nil, err
	}
	// TODO
	//defer cli.Close()
	klog.Info("etcd client is connected")
	return &DBServer{cli: cli,
		uuid:        uuid.NewString(),
		schemas:     make(map[string]string),
		schemaTypes: make(map[string]map[string]map[string]string)}, nil
}

func (con *DBServer) Lock(ctx context.Context, id string) (bool, error) {
	cnx := context.TODO()
	session, err := concurrency.NewSession(con.cli, concurrency.WithContext(cnx))
	if err != nil {
		return false, err
	}
	mutex := concurrency.NewMutex(session, "locks/"+id)
	err = mutex.TryLock(cnx)
	unlock := func() {
		server := jrpc2.ServerFromContext(ctx)
		server.Wait()
		klog.V(5).Infoln("UNLOCK")
		err = mutex.Unlock(cnx)
		if err != nil {
			err = fmt.Errorf("Unlock returned %v\n", err)
		} else {
			klog.V(5).Infoln("UNLOCKED done\n")
		}
	}
	if err == nil {
		go unlock()
		return true, nil
	}
	go func() {
		err = mutex.Lock(cnx)
		if err == nil {
			// Send notification
			klog.V(5).Infoln("Locked")
			go unlock()
			if err := jrpc2.PushNotify(ctx, "locked", []string{id}); err != nil {
				klog.Errorf("notification %v\n", err)
				return

			}
		} else {
			klog.Errorf("Lock error %v\n", err)
		}
	}()
	return false, nil
}

func (con *DBServer) Unlock(ctx context.Context, id string) error {
	cnx := context.TODO()
	session, err := concurrency.NewSession(con.cli, concurrency.WithContext(cnx))
	if err != nil {
		return err
	}
	mutex := concurrency.NewMutex(session, "locks/"+id)
	// TODO
	klog.V(5).Infof("is owner %+v\n", mutex.IsOwner())
	mutex.Unlock(ctx)
	return nil
}

func (con *DBServer) AddSchema(schemaName, schemaFile string) error {
	data, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	con.schemas[schemaName] = string(data)
	return nil
}

func (con *DBServer) LoadServerData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	for schemaName, schema := range con.schemas {
		schemaSet, err := ovsdbjson.NewOvsSet([]string{schema})
		if err != nil {
			cancel = nil
			return err
		}
		srv := _Server.Database{Model: "standalone", Name: schemaName, Uuid: ovsdbjson.UUID{GoUUID: uuid.NewString()},
			Connected: true, Leader: true, Schema: *schemaSet, Version: ovsdbjson.UUID{GoUUID: uuid.NewString()}}
		data, err := json.Marshal(srv)
		if err != nil {
			cancel = nil
			return err
		}
		_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/"+schemaName, string(data))
		if err != nil {
			cancel = nil
			return err
		}
	}

	// OVN_Northbound
	// NB_Global
	_, err := con.cli.Put(ctx, "ovsdb/OVN_Northbound/NB_Global/a5088a51-7756-4dd4-909c-b7c59c9fcce7/connections", "[413afe3e-79ff-4583-88a6-f02b70b8e927]")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/NB_Global/a5088a51-7756-4dd4-909c-b7c59c9fcce7/options", "{e2e_timestamp=\"1612817071\", mac_prefix=\"86:a9:cb\", max_tunid=\"16711680\", northd_internal_version=\"20.12.0-20.14.0-52.0\", northd_probe_interval=\"5000\", svc_monitor_mac=\"5a:d9:62:39:9f:87\"}")

	//ACL
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/action", "allow-related")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/direction", "to-lport")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/match", "ip4.src==10.244.0.2")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/priority", "1001")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/action", "allow-related")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/direction", "to-lport")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/match", "ip4.src==10.244.0.2")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8/priority", "1001")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/3ed181f9-7c68-47ee-bcdc-6cf393a02772/action", "allow-related")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/3ed181f9-7c68-47ee-bcdc-6cf393a02772/direction", "to-lport")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/3ed181f9-7c68-47ee-bcdc-6cf393a02772/match", "ip4.src==10.244.1.2")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/3ed181f9-7c68-47ee-bcdc-6cf393a02772/priority", "1001")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/7071b927-cc6d-4145-8849-395e6226fdac/action", "allow-related")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/7071b927-cc6d-4145-8849-395e6226fdac/direction", "to-lport")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/7071b927-cc6d-4145-8849-395e6226fdac/match", "ip4.src==10.244.1.2")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/ACL/7071b927-cc6d-4145-8849-395e6226fdac/priority", "1001")

	//Address_Set
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/532757d0-bc2e-41b9-bafe-2542f995b011/addresses", "[\"10.244.0.5\"]")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/532757d0-bc2e-41b9-bafe-2542f995b011/external_ids", "{name=local-path-storage_v4}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/532757d0-bc2e-41b9-bafe-2542f995b011/name", "a10956707444534956691")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/8e33c234-2da4-4e5f-858f-4bcd5bc3c68b/external_ids", "{name=default_v4}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/8e33c234-2da4-4e5f-858f-4bcd5bc3c68b/name", "a5154718082306775057")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/3581fd85-1428-45a8-9702-edec71dda0a1/addresses", "[\"10.244.0.3\", \"10.244.0.4\"]")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/3581fd85-1428-45a8-9702-edec71dda0a1/external_ids", "{name=kube-system_v4}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/3581fd85-1428-45a8-9702-edec71dda0a1/name", "a6937002112706621489")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/99ad8ae1-bc86-4662-bca4-a88fd675ee3d/external_ids", "{name=ovn-kubernetes_v4}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/99ad8ae1-bc86-4662-bca4-a88fd675ee3d/name", "a5675285926127865604")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/fde500ad-eff5-47a3-be0b-02e7c23a1357/external_ids", "{name=kube-public_v4}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/fde500ad-eff5-47a3-be0b-02e7c23a1357/name", "a18363165982804349389")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/0af13342-2ea7-486d-825a-b57bd70a8cbc/external_ids", "{name=kube-node-lease_v4}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Address_Set/0af13342-2ea7-486d-825a-b57bd70a8cbc/name", "a16235039932615691331")

	// Connection
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Connection/413afe3e-79ff-4583-88a6-f02b70b8e927/status", "{bound_port=\"6641\", n_connections=\"3\", sec_since_connect=\"0\", sec_since_disconnect=\"0\"}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Connection/413afe3e-79ff-4583-88a6-f02b70b8e927/target", "ptcp:6641:172.18.0.4")

	// Forwarding_Group
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Forwarding_Group/6be9235a-b3b6-41d7-a5aa-356b5b3c96cc/external_ids", "{name=clusterPortGroup}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Forwarding_Group/6be9235a-b3b6-41d7-a5aa-356b5b3c96cc/name", "clusterPortGroup")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Forwarding_Group/6be9235a-b3b6-41d7-a5aa-356b5b3c96cc/ports", "[25f2e69e-4bac-4529-9082-9f94da060cf1, 73000cf3-73d0-4283-8aad-bcf181626a40, be25033c-27df-42a2-9765-52bc06acc71c]")

	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Forwarding_Group/ee4d82d2-3a7d-4737-be8d-656374f5d56c/external_ids", "{name=clusterRtrPortGroup}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Forwarding_Group/ee4d82d2-3a7d-4737-be8d-656374f5d56c/name", "clusterRtrPortGroup")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Forwarding_Group/ee4d82d2-3a7d-4737-be8d-656374f5d56c/ports", "[b4298483-cf17-46d4-9da1-034eab065ff1, b6e1fc02-0306-4887-8e36-e8b0ec22b16c, fcf06a69-16c2-4f34-b3a4-282a641862f8]")

	// Gateway_Chassis
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Gateway_Chassis/99c45e0b-3688-4992-900c-7d5a25930ba3/chassis_name", "1bd76edb-8626-4ecd-8185-788bd2121bda")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Gateway_Chassis/99c45e0b-3688-4992-900c-7d5a25930ba3/external_ids", "{dgp_name=rtos-node_local_switch}")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Gateway_Chassis/99c45e0b-3688-4992-900c-7d5a25930ba3/name", "rtos-node_local_switch_1bd76edb-8626-4ecd-8185-788bd2121bda")
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/Gateway_Chassis/99c45e0b-3688-4992-900c-7d5a25930ba3/priority", "100")

	cancel()
	return err
}

func (con *DBServer) GetData(prefix string, keysOnly bool) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	var resp *clientv3.GetResponse
	var err error
	if keysOnly {
		resp, err = con.cli.Get(ctx, prefix, clientv3.WithFromKey(), clientv3.WithKeysOnly())
	} else {
		resp, err = con.cli.Get(ctx, prefix, clientv3.WithFromKey())
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
func (con *DBServer) GetMarshaled(prefix string, columns []interface{}) (*[]map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	resp, err := con.cli.Get(ctx, prefix, clientv3.WithFromKey())
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
