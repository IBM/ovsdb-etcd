package ovsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ibm/ovsdb-etcd/pkg/types/OVN_Northbound"
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

type DBServerInterface interface {
	Lock(ctx context.Context, id string) (bool, error)
	Unlock(ctx context.Context, id string) error
	AddSchema(schemaName, schemaFile string) error
	LoadServerData() error
	GetData(prefix string, keysOnly bool) (*clientv3.GetResponse, error)
	GetMarshaled(prefix string, columns []interface{}) (*[]map[string]string, error)
	GetSchema(name string) (string, bool)
	GetUUID() string
}

type DBServer struct {
	cli         *clientv3.Client
	uuid        string
	schemas     map[string]string
	schemaTypes map[string]map[string]map[string]string
}

func NewDBServer(endpoints []string) (DBServerInterface, error) {
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
	buffer := new(bytes.Buffer)
	json.Compact(buffer, data)
	con.schemas[schemaName] = buffer.String()
	return nil
}

// TODO, code for development, will be removed
func (con *DBServer) LoadServerData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Millisecond)
	putOnEtcd := func(key string, obj interface{}) error {
		if cancel == nil {
			return errors.New("cancel is nil") // TODO understand the meaning of this varible and if this check is needed
		}
		data, err := json.Marshal(obj)
		if err != nil {
			cancel = nil
			return err
		}
		_, err = con.cli.Put(ctx, key, string(data))
		if err != nil {
			cancel = nil
			return err
		}
		return nil
	}
	for schemaName, schema := range con.schemas {
		schemaSet, err := ovsdbjson.NewOvsSet([]string{schema})
		if err != nil {
			cancel = nil
			return err
		}
		srv := _Server.Database{Model: "standalone", Name: schemaName, Uuid: ovsdbjson.UUID{GoUUID: uuid.NewString()},
			Connected: true, Leader: true, Schema: *schemaSet, Version: ovsdbjson.UUID{GoUUID: uuid.NewString()}}
		putOnEtcd("ovsdb/_Server/Database/"+schemaName, srv)
	}

	// OVN_Northbound
	// NB_Global
	nbConnectionsSet, err := ovsdbjson.NewOvsSet([]string{"413afe3e-79ff-4583-88a6-f02b70b8e927"})
	if err != nil {
		cancel = nil
		return err
	}
	nbOptionsMap, err := ovsdbjson.NewOvsMap(map[string]string{
		"e2e_timestamp":           "1612817071",
		"mac_prefix":              "86:a9:cb",
		"max_tunid":               "16711680",
		"northd_internal_version": "20.12.0-20.14.0-52.0",
		"northd_probe_interval":   "5000",
		"svc_monitor_mac":         "5a:d9:62:39:9f:87",
	})
	if err != nil {
		cancel = nil
		return err
	}

	nbGlobal := OVN_Northbound.NB_Global{
		Connections:      *nbConnectionsSet,
		External_ids:     ovsdbjson.OvsMap{},
		Hv_cfg:           0,
		Hv_cfg_timestamp: 0,
		Ipsec:            false,
		Name:             "",
		Nb_cfg:           0,
		Nb_cfg_timestamp: 0,
		Options:          *nbOptionsMap,
		Sb_cfg:           0,
		Sb_cfg_timestamp: 0,
		Ssl:              ovsdbjson.OvsSet{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "a5088a51-7756-4dd4-909c-b7c59c9fcce7"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/NB_Global/a5088a51-7756-4dd4-909c-b7c59c9fcce7", nbGlobal)

	//ACL
	acl1 := OVN_Northbound.ACL{
		Action:       "allow-related",
		Direction:    "to-lport",
		External_ids: ovsdbjson.OvsMap{},
		Log:          false,
		Match:        "ip4.src==10.244.0.2",
		Meter:        ovsdbjson.OvsSet{},
		Name:         ovsdbjson.OvsSet{},
		Priority:     1001,
		Severity:     ovsdbjson.OvsSet{},
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "aa2bab19-9b31-4d01-b1ad-f5e49dd269f8"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/ACL/aa2bab19-9b31-4d01-b1ad-f5e49dd269f8", acl1)

	acl2 := OVN_Northbound.ACL{
		Action:       "allow-related",
		Direction:    "to-lport",
		External_ids: ovsdbjson.OvsMap{},
		Log:          false,
		Match:        "ip4.src==10.244.1.2",
		Meter:        ovsdbjson.OvsSet{},
		Name:         ovsdbjson.OvsSet{},
		Priority:     1001,
		Severity:     ovsdbjson.OvsSet{},
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "3ed181f9-7c68-47ee-bcdc-6cf393a02772"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/ACL/3ed181f9-7c68-47ee-bcdc-6cf393a02772", acl2)

	acl3 := OVN_Northbound.ACL{
		Action:       "allow-related",
		Direction:    "to-lport",
		External_ids: ovsdbjson.OvsMap{},
		Log:          false,
		Match:        "ip4.src==10.244.1.2",
		Meter:        ovsdbjson.OvsSet{},
		Name:         ovsdbjson.OvsSet{},
		Priority:     1001,
		Severity:     ovsdbjson.OvsSet{},
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "7071b927-cc6d-4145-8849-395e6226fdac"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/ACL/7071b927-cc6d-4145-8849-395e6226fdac", acl3)

	//Address_Set

	addressesSet1, err := ovsdbjson.NewOvsSet([]string{"10.244.0.5"}) // TODO understand if to keep or remove the square brackets and Quotation mark "
	if err != nil {
		cancel = nil
		return err
	}
	externalIdsMap1, err := ovsdbjson.NewOvsMap(map[string]string{
		"name": "local-path-storage_v4",
	})
	if err != nil {
		cancel = nil
		return err
	}
	addressSet1 := OVN_Northbound.Address_Set{
		Addresses:    *addressesSet1,
		External_ids: *externalIdsMap1,
		Name:         "a10956707444534956691",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "532757d0-bc2e-41b9-bafe-2542f995b011"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Address_Set/532757d0-bc2e-41b9-bafe-2542f995b011", addressSet1)

	externalIdsMap2, err := ovsdbjson.NewOvsMap(map[string]string{
		"name": "local-path-storage_v4",
	})
	if err != nil {
		cancel = nil
		return err
	}
	addressSet2 := OVN_Northbound.Address_Set{
		Addresses:    ovsdbjson.OvsSet{},
		External_ids: *externalIdsMap2,
		Name:         "a5154718082306775057",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "8e33c234-2da4-4e5f-858f-4bcd5bc3c68b"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Address_Set/8e33c234-2da4-4e5f-858f-4bcd5bc3c68b", addressSet2)

	addressesSet3, err := ovsdbjson.NewOvsSet([]string{"10.244.0.3", "10.244.0.4"}) // TODO understand if to keep or remove the square brackets and Quotation mark "
	if err != nil {
		cancel = nil
		return err
	}
	externalIdsMap3, err := ovsdbjson.NewOvsMap(map[string]string{
		"name": "kube-system_v4",
	})
	if err != nil {
		cancel = nil
		return err
	}
	addressSet3 := OVN_Northbound.Address_Set{
		Addresses:    *addressesSet3,
		External_ids: *externalIdsMap3,
		Name:         "a6937002112706621489",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "3581fd85-1428-45a8-9702-edec71dda0a1"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Address_Set/3581fd85-1428-45a8-9702-edec71dda0a1", addressSet3)

	externalIdsMap4, err := ovsdbjson.NewOvsMap(map[string]string{
		"name": "ovn-kubernetes_v4",
	})
	if err != nil {
		cancel = nil
		return err
	}
	addressSet4 := OVN_Northbound.Address_Set{
		Addresses:    ovsdbjson.OvsSet{},
		External_ids: *externalIdsMap4,
		Name:         "a5675285926127865604",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "99ad8ae1-bc86-4662-bca4-a88fd675ee3d"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Address_Set/99ad8ae1-bc86-4662-bca4-a88fd675ee3d", addressSet4)

	externalIdsMap5, err := ovsdbjson.NewOvsMap(map[string]string{
		"name": "kube-public_v4",
	})
	if err != nil {
		cancel = nil
		return err
	}
	addressSet5 := OVN_Northbound.Address_Set{
		Addresses:    ovsdbjson.OvsSet{},
		External_ids: *externalIdsMap5,
		Name:         "a18363165982804349389",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "fde500ad-eff5-47a3-be0b-02e7c23a1357"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Address_Set/fde500ad-eff5-47a3-be0b-02e7c23a1357", addressSet5)

	externalIdsMap6, err := ovsdbjson.NewOvsMap(map[string]string{
		"name": "kube-node-lease_v4",
	})
	if err != nil {
		cancel = nil
		return err
	}
	addressSet6 := OVN_Northbound.Address_Set{
		Addresses:    ovsdbjson.OvsSet{},
		External_ids: *externalIdsMap6,
		Name:         "a16235039932615691331",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "2ea7-486d-825a-b57bd70a8cbc"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Address_Set/0af13342-2ea7-486d-825a-b57bd70a8cbc", addressSet6)

	// Connection

	statusMap, err := ovsdbjson.NewOvsMap(map[string]string{"bound_port": "6641", "n_connections": "3", "sec_since_connect": "0", "sec_since_disconnect": "0"})

	connection1 := OVN_Northbound.Connection{
		External_ids:     ovsdbjson.OvsMap{},
		Inactivity_probe: ovsdbjson.OvsSet{},
		Is_connected:     false,
		Max_backoff:      ovsdbjson.OvsSet{},
		Other_config:     ovsdbjson.OvsMap{},
		Status:           *statusMap,
		Target:           "ptcp:6641:172.18.0.4",
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "413afe3e-79ff-4583-88a6-f02b70b8e927"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Connection/413afe3e-79ff-4583-88a6-f02b70b8e927", connection1)

	// Forwarding_Group

	childPortsSet1, err := ovsdbjson.NewOvsSet([]string{"25f2e69e-4bac-4529-9082-9f94da060cf1", "73000cf3-73d0-4283-8aad-bcf181626a40", "be25033c-27df-42a2-9765-52bc06acc71c"}) // TODO understand if to keep or remove the square brackets and Quotation mark "
	if err != nil {
		cancel = nil
		return err
	}
	externalIdsMap1, err = ovsdbjson.NewOvsMap(map[string]string{
		"name": "clusterPortGroup",
	})
	if err != nil {
		cancel = nil
		return err
	}

	fowardingGroup1 := OVN_Northbound.Forwarding_Group{
		Child_port:   *childPortsSet1,
		External_ids: *externalIdsMap1,
		Liveness:     false,
		Name:         "clusterPortGroup",
		Vip:          "",
		Vmac:         "",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "6be9235a-b3b6-41d7-a5aa-356b5b3c96cc"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Forwarding_Group/6be9235a-b3b6-41d7-a5aa-356b5b3c96cc", fowardingGroup1)

	childPortsSet2, err := ovsdbjson.NewOvsSet([]string{"b4298483-cf17-46d4-9da1-034eab065ff1", "b6e1fc02-0306-4887-8e36-e8b0ec22b16c", "fcf06a69-16c2-4f34-b3a4-282a641862f8"}) // TODO understand if to keep or remove the square brackets and Quotation mark "
	if err != nil {
		cancel = nil
		return err
	}
	externalIdsMap2, err = ovsdbjson.NewOvsMap(map[string]string{
		"name": "clusterRtrPortGroup",
	})
	if err != nil {
		cancel = nil
		return err
	}

	fowardingGroup2 := OVN_Northbound.Forwarding_Group{
		Child_port:   *childPortsSet2,
		External_ids: *externalIdsMap2,
		Liveness:     false,
		Name:         "clusterRtrPortGroup",
		Vip:          "",
		Vmac:         "",
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "ee4d82d2-3a7d-4737-be8d-656374f5d56c"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Forwarding_Group/ee4d82d2-3a7d-4737-be8d-656374f5d56c", fowardingGroup2)

	// Gateway_Chassis

	externalIdsMap1, err = ovsdbjson.NewOvsMap(map[string]string{"dgp_name": "rtos-node_local_switch"})
	if err != nil {
		cancel = nil
		return err
	}
	gatewayChassis1 := OVN_Northbound.Gateway_Chassis{
		Chassis_name: "1bd76edb-8626-4ecd-8185-788bd2121bda",
		External_ids: *externalIdsMap1,
		Name:         "rtos-node_local_switch_1bd76edb-8626-4ecd-8185-788bd2121bda",
		Options:      ovsdbjson.OvsMap{},
		Priority:     100,
		Version:      ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:         ovsdbjson.UUID{GoUUID: "99c45e0b-3688-4992-900c-7d5a25930ba3"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Gateway_Chassis/99c45e0b-3688-4992-900c-7d5a25930ba3", gatewayChassis1)

	// Load_Balancer

	tcpProtocolSet, err := ovsdbjson.NewOvsSet([]string{"tcp"})
	if err != nil {
		cancel = nil
		return err
	}
	externalIdsMap1, err = ovsdbjson.NewOvsMap(map[string]string{"k8s-cluster-lb-tcp": "yes"})
	if err != nil {
		cancel = nil
		return err
	}
	vipsMap1, err := ovsdbjson.NewOvsMap(map[string]string{"10.96.0.10:53": "10.244.0.3:53,10.244.0.4:53",
		"10.96.0.10:9153": "10.244.0.3:9153,10.244.0.4:9153", "10.96.0.1:443": "172.18.0.4:6443"})
	if err != nil {
		cancel = nil
		return err
	}
	loadBalancer1 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap1,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *tcpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             *vipsMap1,
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "32cc16f0-cda7-4c63-87d7-d30349ce32d7"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/32cc16f0-cda7-4c63-87d7-d30349ce32d7", loadBalancer1)

	udpProtocolSet, err := ovsdbjson.NewOvsSet([]string{"udp"})
	if err != nil {
		cancel = nil
		return err
	}

	externalIdsMap2, err = ovsdbjson.NewOvsMap(map[string]string{"k8s-cluster-lb-udp": "yes"})
	if err != nil {
		cancel = nil
		return err
	}

	vipsMap2, err := ovsdbjson.NewOvsMap(map[string]string{"10.96.0.10:53": "10.244.0.3:53,10.244.0.4:53"})

	loadBalancer2 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap2,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *udpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             *vipsMap2,
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "65bd48d9-5d2b-4dd3-a993-50876a5f4a05"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/65bd48d9-5d2b-4dd3-a993-50876a5f4a05", loadBalancer2)

	sctpProtocolSet, err := ovsdbjson.NewOvsSet([]string{"sctp"})
	if err != nil {
		cancel = nil
		return err
	}
	externalIdsMap3, err = ovsdbjson.NewOvsMap(map[string]string{"SCTP_lb_gateway_router": "GR_ovn-control-plane"})
	if err != nil {
		cancel = nil
		return err
	}

	loadBalancer3 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap3,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *sctpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "a6850cf1-5aee-462c-9195-5397d6417938"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/a6850cf1-5aee-462c-9195-5397d6417938", loadBalancer3)

	externalIdsMap4, err = ovsdbjson.NewOvsMap(map[string]string{"TCP_lb_gateway_router": "GR_ovn-worker2"})
	if err != nil {
		cancel = nil
		return err
	}

	loadBalancer4 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap4,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *tcpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "53e13052-4a7e-4857-be8f-c234d978482f"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/53e13052-4a7e-4857-be8f-c234d978482f", loadBalancer4)

	externalIdsMap5, err = ovsdbjson.NewOvsMap(map[string]string{"UDP_lb_gateway_router": "GR_ovn-worker2"})
	if err != nil {
		cancel = nil
		return err
	}

	loadBalancer5 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap5,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *udpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "6be22262-6c30-4cd3-929b-23e79f90182f"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/6be22262-6c30-4cd3-929b-23e79f90182f", loadBalancer5)

	externalIdsMap6, err = ovsdbjson.NewOvsMap(map[string]string{"UDP_lb_gateway_router": "GR_ovn-worker2"})
	if err != nil {
		cancel = nil
		return err
	}

	loadBalancer6 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap6,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *tcpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "3f00965b-7930-4232-8cee-cf877ca1da04"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/3f00965b-7930-4232-8cee-cf877ca1da04", loadBalancer6)

	externalIdsMap7, err := ovsdbjson.NewOvsMap(map[string]string{"k8s-cluster-lb-sctp": "yes"})
	if err != nil {
		cancel = nil
		return err
	}
	loadBalancer7 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap7,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *sctpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "d7b47c1b-55bc-45a9-bb0d-4032e327c3d2"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/d7b47c1b-55bc-45a9-bb0d-4032e327c3d2", loadBalancer7)

	externalIdsMap8, err := ovsdbjson.NewOvsMap(map[string]string{"UDP_lb_gateway_router": "GR_ovn-control-plane"})
	if err != nil {
		cancel = nil
		return err
	}
	loadBalancer8 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap8,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *udpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "d8df5624-a22e-447b-95cc-fc40f60b6aa4"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/d8df5624-a22e-447b-95cc-fc40f60b6aa4", loadBalancer8)

	externalIdsMap9, err := ovsdbjson.NewOvsMap(map[string]string{"UDP_lb_gateway_router": "GR_ovn-worker"})
	if err != nil {
		cancel = nil
		return err
	}

	loadBalancer9 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap9,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *udpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "667a3465-d536-4561-9daa-210a32ad7a5a"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/667a3465-d536-4561-9daa-210a32ad7a5a", loadBalancer9)

	externalIdsMap10, err := ovsdbjson.NewOvsMap(map[string]string{"TCP_lb_gateway_router": "GR_ovn-control-plane"})
	if err != nil {
		cancel = nil
		return err
	}
	loadBalancer10 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap10,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *tcpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "0c0d7ca5-c56d-4130-9e8f-5d8ffb3a5915"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/0c0d7ca5-c56d-4130-9e8f-5d8ffb3a5915", loadBalancer10)

	externalIdsMap11, err := ovsdbjson.NewOvsMap(map[string]string{"SCTP_lb_gateway_router": "GR_ovn-worker"})
	if err != nil {
		cancel = nil
		return err
	}
	loadBalancer11 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap11,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *sctpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "3a2be97e-8be5-44c5-91b9-a93e1767e374"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/3a2be97e-8be5-44c5-91b9-a93e1767e374", loadBalancer11)

	externalIdsMap12, err := ovsdbjson.NewOvsMap(map[string]string{"SCTP_lb_gateway_router": "GR_ovn-worker2"})
	if err != nil {
		cancel = nil
		return err
	}
	loadBalancer12 := OVN_Northbound.Load_Balancer{
		External_ids:     *externalIdsMap12,
		Health_check:     ovsdbjson.OvsSet{},
		Ip_port_mappings: ovsdbjson.OvsMap{},
		Name:             "",
		Options:          ovsdbjson.OvsMap{},
		Protocol:         *sctpProtocolSet,
		Selection_fields: ovsdbjson.OvsSet{},
		Vips:             ovsdbjson.OvsMap{},
		Version:          ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:             ovsdbjson.UUID{GoUUID: "90f32ca6-e752-4ea9-9e49-6ff3d3cc4e18"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Load_Balancer/90f32ca6-e752-4ea9-9e49-6ff3d3cc4e18", loadBalancer12)

	// Logical_Router

	externalIdsMap1, err = ovsdbjson.NewOvsMap(map[string]string{"physical_ip": "172.18.0.4", "physical_ips": "172.18.0.4"})
	if err != nil {
		cancel = nil
		return err
	}
	logicalRouterOptionsMap1, err := ovsdbjson.NewOvsMap(map[string]string{"always_learn_from_arp_request": "false", "chassi": "1bd76edb-8626-4ecd-8185-788bd2121bda", "dynamic_neigh_router": "true"})
	if err != nil {
		cancel = nil
		return err
	}
	portsSet1, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "af4e1844-8479-476f-a45a-6444475f0062"}, ovsdbjson.UUID{GoUUID: "d54fc12c-ecd5-44c6-a2b9-bda4e0534d6b"}})

	if err != nil {
		cancel = nil
		return err
	}

	staticRoutesSet1, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "7115ddef-8cad-4fc9-8471-63715480e4fd"}, ovsdbjson.UUID{GoUUID: "e4c2100f-f7ba-4129-a11f-0b4d854a7c28"}})

	if err != nil {
		cancel = nil
		return err
	}

	logicalRouter1 := OVN_Northbound.Logical_Router{
		Enabled:       ovsdbjson.OvsSet{},
		External_ids:  *externalIdsMap1,
		Load_balancer: ovsdbjson.OvsSet{},
		Name:          "GR_ovn-control-plane",
		Nat:           ovsdbjson.OvsSet{},
		Options:       *logicalRouterOptionsMap1,
		Policies:      ovsdbjson.OvsSet{},
		Ports:         *portsSet1,
		Static_routes: *staticRoutesSet1,
		Version:       ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:          ovsdbjson.UUID{GoUUID: "22c3143c-15c8-4018-91b6-8fe4b0ffab80"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Logical_Router/22c3143c-15c8-4018-91b6-8fe4b0ffab80", logicalRouter1)

	externalIdsMap2, err = ovsdbjson.NewOvsMap(map[string]string{"physical_ip": "172.18.0.2", "physical_ips": "172.18.0.2"})
	if err != nil {
		cancel = nil
		return err
	}
	logicalRouterOptionsMap2, err := ovsdbjson.NewOvsMap(map[string]string{"always_learn_from_arp_request": "false", "chassis": "8ec06983-c3c3-4687-b2b0-6283ee76b252", "dynamic_neigh_routers": "true"})
	if err != nil {
		cancel = nil
		return err
	}
	portsSet2, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "a3be4bc9-9bd0-496f-9796-afc43f6cae12"}, ovsdbjson.UUID{GoUUID: "a5731950-de12-4d55-8687-5fa7d2571742"}})
	if err != nil {
		cancel = nil
		return err
	}

	staticRoutesSet2, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "815544ad-3bd9-4224-8776-75a3effabe28"}, ovsdbjson.UUID{GoUUID: "93431fc9-c663-4ca9-9e7c-bbf964c39471"}})

	if err != nil {
		cancel = nil
		return err
	}

	logicalRouter2 := OVN_Northbound.Logical_Router{
		Enabled:       ovsdbjson.OvsSet{},
		External_ids:  *externalIdsMap2,
		Load_balancer: ovsdbjson.OvsSet{},
		Name:          "GR_ovn-worker2",
		Nat:           ovsdbjson.OvsSet{},
		Options:       *logicalRouterOptionsMap2,
		Policies:      ovsdbjson.OvsSet{},
		Ports:         *portsSet2,
		Static_routes: *staticRoutesSet2,
		Version:       ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:          ovsdbjson.UUID{GoUUID: "70f7ff95-16c1-4832-9073-5c5ba807d205"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Logical_Router/70f7ff95-16c1-4832-9073-5c5ba807d205", logicalRouter2)

	externalIdsMap3, err = ovsdbjson.NewOvsMap(map[string]string{"physical_ip": "172.18.0.3", "physical_ips": "172.18.0.3"})
	if err != nil {
		cancel = nil
		return err
	}
	logicalRouterOptionsMap3, err := ovsdbjson.NewOvsMap(map[string]string{"always_learn_from_arp_request": "false", "chassis": "451b020e-f24f-4b5f-929c-79e91455893d", "dynamic_neigh_routers": "true"})
	if err != nil {
		cancel = nil
		return err
	}
	portsSet3, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "28997de6-8ea1-4338-80de-72fde80c8d95"}, ovsdbjson.UUID{GoUUID: "7268435c-3a36-4db8-aef8-a194f2edd2bd"}})
	if err != nil {
		cancel = nil
		return err
	}
	staticRoutesSet3, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "31b69005-4c12-4555-8103-e42ffc1a7abf"}, ovsdbjson.UUID{GoUUID: "a4c5198c-417a-4c00-9d8e-799c0b603f0e"}})
	if err != nil {
		cancel = nil
		return err
	}

	logicalRouter3 := OVN_Northbound.Logical_Router{
		Enabled:       ovsdbjson.OvsSet{},
		External_ids:  *externalIdsMap3,
		Load_balancer: ovsdbjson.OvsSet{},
		Name:          "GR_ovn-worker",
		Nat:           ovsdbjson.OvsSet{},
		Options:       *logicalRouterOptionsMap3,
		Policies:      ovsdbjson.OvsSet{},
		Ports:         *portsSet3,
		Static_routes: *staticRoutesSet3,
		Version:       ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:          ovsdbjson.UUID{GoUUID: "a172a05d-7260-4a5c-b38b-9bd989ee6e9c"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Logical_Router/a172a05d-7260-4a5c-b38b-9bd989ee6e9c", logicalRouter3)

	externalIdsMap4, err = ovsdbjson.NewOvsMap(map[string]string{"k8s-cluster-router": "yes", "k8s-ovn-topo-version": "2"})
	if err != nil {
		cancel = nil
		return err
	}

	natSet4, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{
		ovsdbjson.UUID{GoUUID: "66632550-9c14-4503-b5d9-a41e0e424af9"},
		ovsdbjson.UUID{GoUUID: "9bef6aa1-77bb-4b8d-9bc1-9cf0e3e6d53f"},
		ovsdbjson.UUID{GoUUID: "d2c8158d-7a13-46b1-80a7-85383138a188"}})
	if err != nil {
		cancel = nil
		return err
	}

	policiesSet4, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{
		ovsdbjson.UUID{GoUUID: "02f2f106-9b5c-4f03-9026-475a6ca6159d"},
		ovsdbjson.UUID{GoUUID: "035fbfeb-0972-4f53-a0f6-493c67223370"},
		ovsdbjson.UUID{GoUUID: "242653ec-3efe-47f5-8225-b8595465b6ef"},
		ovsdbjson.UUID{GoUUID: "2e04ae7b-a706-4dff-95f7-9d83de2529c0"},
		ovsdbjson.UUID{GoUUID: "5c568adf-2518-43e0-be1e-6d4efd26490a"},
		ovsdbjson.UUID{GoUUID: "5e9cf2a3-e9e1-4b97-b3e6-6c18bd338b75"},
		ovsdbjson.UUID{GoUUID: "60b80577-782a-43c3-ba89-b89dbe8b60e7"},
		ovsdbjson.UUID{GoUUID: "8c506059-dba8-415d-a84d-6cd1af3eeed6"},
		ovsdbjson.UUID{GoUUID: "bf157b54-c8f6-4e45-95c7-6bebff4d63e9"},
		ovsdbjson.UUID{GoUUID: "dfd28891-5aa8-4508-b37b-f3145f6ee8fe"},
		ovsdbjson.UUID{GoUUID: "e70b41f0-5575-4c04-b435-80016beb87a0"},
		ovsdbjson.UUID{GoUUID: "ee614326-2326-4d57-8721-58a659fcbd6e"},
		ovsdbjson.UUID{GoUUID: "ffe013b0-3974-415c-b67f-fe7dc34628e3"},
	})
	if err != nil {
		cancel = nil
		return err
	}
	portsSet4, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "0028e530-18da-4b1d-bec3-ca6f4f8954b5"}, ovsdbjson.UUID{GoUUID: "18502ff7-e896-4bba-989b-3548267848a8"},
		ovsdbjson.UUID{GoUUID: "6b7d2637-cee1-481c-8273-523ca7d99d9d"}, ovsdbjson.UUID{GoUUID: "d293e3dc-3cca-499c-9bce-0c2d619f6df7"},
		ovsdbjson.UUID{GoUUID: "d4171d97-758d-4ce1-a7e8-2daf3f872844"}})
	if err != nil {
		cancel = nil
		return err
	}

	staticRoutesSet4, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "01c3bc10-332e-4668-9bd9-f67e63c53afd"}, ovsdbjson.UUID{GoUUID: "1b4c524c-a36f-41d0-8bcf-cbd3e5f8171a"},
		ovsdbjson.UUID{GoUUID: "4a828663-b619-4470-a8e3-59f5977e88ec"}, ovsdbjson.UUID{GoUUID: "65215a82-3c7d-49d3-9ebd-62bc978bb457"},
		ovsdbjson.UUID{GoUUID: "6aeb90da-9ee7-4947-ac18-9ad742a1008f"}, ovsdbjson.UUID{GoUUID: "b57fbccd-675e-471d-8aed-8b279f0d65eb"},
		ovsdbjson.UUID{GoUUID: "baa3b44e-3831-4575-a3f5-80bb6ec2910f"}, ovsdbjson.UUID{GoUUID: "dac55e3b-6b15-46bf-b8eb-9431d963f3d8"},
		ovsdbjson.UUID{GoUUID: "fe4089a4-33f1-4f82-b8bb-aaa01e027c5c"}})

	logicalRouter4 := OVN_Northbound.Logical_Router{
		Enabled:       ovsdbjson.OvsSet{},
		External_ids:  *externalIdsMap4,
		Load_balancer: ovsdbjson.OvsSet{},
		Name:          "ovn_cluster_router",
		Nat:           *natSet4,
		Options:       ovsdbjson.OvsMap{},
		Policies:      *policiesSet4,
		Ports:         *portsSet4,
		Static_routes: *staticRoutesSet4,
		Version:       ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:          ovsdbjson.UUID{GoUUID: "320b8a23-fccd-4737-96e5-e823e741c5ac"},
	}

	putOnEtcd("ovsdb/OVN_Northbound/Logical_Router/320b8a23-fccd-4737-96e5-e823e741c5ac", logicalRouter4)

	// Logical_Switch (only 5 and 6)

	portsSet5, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "006d780f-7169-44f6-8eb2-9df757feef61"}, ovsdbjson.UUID{GoUUID: "a8fe81ba-4746-4d6f-aebe-d53cfd4ae46f"}})
	if err != nil {
		cancel = nil
		return err
	}
	logicalSwitch5 := OVN_Northbound.Logical_Switch{
		Acls:              ovsdbjson.OvsSet{},
		Dns_records:       ovsdbjson.OvsSet{},
		External_ids:      ovsdbjson.OvsMap{},
		Forwarding_groups: ovsdbjson.OvsSet{},
		Load_balancer:     ovsdbjson.OvsSet{},
		Name:              "ext_ovn-control-plane",
		Other_config:      ovsdbjson.OvsMap{},
		Ports:             *portsSet5,
		Qos_rules:         ovsdbjson.OvsSet{},
		Version:           ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:              ovsdbjson.UUID{GoUUID: "4cccc9ad-5ba6-42f9-a749-2d5f3c54ace4"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Logical_Switch/4cccc9ad-5ba6-42f9-a749-2d5f3c54ace4", logicalSwitch5)
	portsSet6, err := ovsdbjson.NewOvsSet([]ovsdbjson.UUID{ovsdbjson.UUID{GoUUID: "2a77a299-b713-4461-b3f6-4e91485fae79"}, ovsdbjson.UUID{GoUUID: "e172ea88-8779-4fec-8fb4-553efb426880"}})
	if err != nil {
		cancel = nil
		return err
	}
	logicalSwitch6 := OVN_Northbound.Logical_Switch{
		Acls:              ovsdbjson.OvsSet{},
		Dns_records:       ovsdbjson.OvsSet{},
		External_ids:      ovsdbjson.OvsMap{},
		Forwarding_groups: ovsdbjson.OvsSet{},
		Load_balancer:     ovsdbjson.OvsSet{},
		Name:              "ext_ovn-worker",
		Other_config:      ovsdbjson.OvsMap{},
		Ports:             *portsSet6,
		Qos_rules:         ovsdbjson.OvsSet{},
		Version:           ovsdbjson.UUID{GoUUID: uuid.NewString()},
		Uuid:              ovsdbjson.UUID{GoUUID: "1822aa4e-9d58-4261-a22b-ecb6f864a3bc"},
	}
	putOnEtcd("ovsdb/OVN_Northbound/Logical_Switch/1822aa4e-9d58-4261-a22b-ecb6f864a3bc", logicalSwitch6)

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

func (con *DBServer) GetSchema(name string) (string, bool) {
	return con.schemas[name], true
}

func (con *DBServer) GetUUID() string {
	return con.uuid
}

type DBServerMock struct {
}

func NewDBServerMock() (DBServerInterface, error) {
	return &DBServerMock{}, nil
}

func (con *DBServerMock) Lock(ctx context.Context, id string) (bool, error) {
	return true, nil
}

func (con *DBServerMock) Unlock(ctx context.Context, id string) error {
	return nil
}

func (con *DBServerMock) AddSchema(schemaName, schemaFile string) error {
	return nil
}

func (con *DBServerMock) LoadServerData() error {
	return nil
}

func (con *DBServerMock) GetData(prefix string, keysOnly bool) (*clientv3.GetResponse, error) {
	var resp *clientv3.GetResponse
	return resp, nil
}

func (con *DBServerMock) GetMarshaled(prefix string, columns []interface{}) (*[]map[string]string, error) {
	return &[]map[string]string{}, nil
}

func (con *DBServerMock) GetSchema(name string) (string, bool) {
	return "", true
}

func (con *DBServerMock) GetUUID() string {
	return ""
}
