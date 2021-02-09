package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"strings"
	"time"
)

type DBServer struct {
	cli *clientv3.Client
	schemas map[string]string
	schemaTypes map[string]map[string]map[string]string
}

func NewDBServer( endpoints []string) (*DBServer, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:  endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("NewETCDConenctor , error: ", err)
		return nil, err
	}
	// TODO
	//defer cli.Close()
	fmt.Println("etcd client is connected")
	return &DBServer{cli: cli,
		schemas:make(map[string]string),
	    schemaTypes: make(map[string]map[string]map[string]string)}, nil
}

 func (con *DBServer)AddSchema(schemaName, schemaFile string) error {
	data, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	con.schemas[schemaName] =  string(data)

	var schemaTypes map[string]map[string]string
	var ok bool
	if schemaTypes, ok = con.schemaTypes[schemaName]; !ok {
		schemaTypes = make(map[string]map[string]string)
	}

	var s interface{}
	err = json.Unmarshal(data, &s)
	if err != nil {
		 return err
	}
	m := s.(map[string]interface{})
	tables, ok := m["tables"]
	if !ok {
	 	// TODO
	 	fmt.Printf("Schema doesn't have tables")
	 	return nil
	}
	tMap := tables.(map[string]interface{})
	for tName, table := range tMap {
	 	tabMap := table.(map[string]interface{})
	 	columnTypesMap := make(map[string]string)
	 	columns, ok := tabMap["columns"]
		 if !ok {
			 // TODO
			 fmt.Printf("Table doesn't have columns")
			 continue
		 }
		schemaTypes[tName] = columnTypesMap
		 columnsmap := columns.(map[string]interface{})
		 for k, v := range columnsmap {
		 	fmt.Printf(" Table %s column %s \n", tName, k)
		 	colValMap := v.(map[string]interface{})
		 	keyEntry, ok := colValMap["type"]
			 if !ok {
				 // TODO
				 fmt.Printf("------------ The column %s doesn't have key type\n", k)
				 continue
			 }

				// fmt.Printf(" Table %s key %s value -->%s %T %v\n", tName, k, cvn, cvv, cvv)
				 switch keyEntry.(type) {
				 case string:
					 fmt.Printf("!!!  Table %s column %s type %s\n", tName, k, keyEntry)
					 columnsmap[k] = keyEntry.(string)
				 case map[string]interface {}:
				 	m2 := keyEntry.(map[string]interface {})
					key, ok := m2["key"]
					if !ok {
						 // TODO
						 fmt.Printf("------------ The column %s doesn't have key type.key, we'll try type\n", k)
						 t := m2["type"]
						 switch t.(type) {
						 case string:
							 fmt.Printf("!!!  Table %s column %s type %s\n", tName, k, t)
							 columnsmap[k] = t.(string)
						 default:
							 fmt.Printf("$$$$  Table %s column %s type %T %s\n", tName, k, t, t)
						 }
						 continue
					 }
					 value, okV := m2["value"]
					 if okV {
					 	var mapKey string
						 switch key.(type) {
						 case string:
							 mapKey = key.(string)
						 case map[string]interface{}:
							 k2Map := key.(map[string]interface{})
							 mapKey = k2Map["type"].(string)
						 default:
							 fmt.Printf(" $$$$=> Table %s column  %s keyType --> %T %v\n", tName, k, key, key)
						 }
					 	// we support only maps with string keys
					 	switch value.(type) {
						case string:
							fmt.Printf("!!!  Table %s column %s type %s\n", tName, k, "map[" + mapKey + "]" + value.(string))

						case map[string]interface{}:
							valueMap := value.(map[string]interface{})
							fmt.Printf("!!!  Table %s column %s type %s\n", tName, k, "map[" + mapKey + "]" + valueMap["type"].(string))
						default:
							fmt.Printf(" $$$$=> Table %s column  %s keyType --> %T %v\n", tName, k, value, value)
						}
						 continue
					 }
					 switch key.(type) {
					 case string:
						 fmt.Printf("!!!  Table %s column %s type %s\n", tName, k, key)
						 continue
					 case map[string]interface{}:
						 k2Map := key.(map[string]interface{})

						 /*for k2, v2 := range k2Map {
						 	 fmt.Printf(" Table %s key %s value -+-+> %v  key = %v value =  %T %v\n", tName, k, keyEntry, k2, v2, v2)
						  }*/
						 fmt.Printf("!!!2222  Table %s column %s type %s\n", tName, k, k2Map["type"])
					 default:
						 fmt.Printf(" ===> Table %s column  %s keyType --> %T %v\n", tName, k, key, key)
					 }

				 default:
					 fmt.Printf(" ===> Table %s key %s value --> %T %v\n", tName, k, keyEntry, keyEntry)
				 }

		 }
	 }
	return nil
}

func (con *DBServer) LoadServerData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/name", "_Server")
	if err!= nil{
		return err
	}
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/model", "standalone")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/connected", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/leader", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/schema", con.schemas["_Server"])

	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/name", "OVN_Northbound")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/model", "standalone")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/connected", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/leader", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/schema", con.schemas["OVN_Northbound"])

	// OVN_Northbound
	// NB_Global
	_, err = con.cli.Put(ctx, "ovsdb/OVN_Northbound/NB_Global/a5088a51-7756-4dd4-909c-b7c59c9fcce7/connections", "[413afe3e-79ff-4583-88a6-f02b70b8e927]")
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

func (con *DBServer) GetData(prefix string) (*clientv3.GetResponse, error) {
	fmt.Printf("GetData " + prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	resp, err := con.cli.Get(ctx, prefix,clientv3.WithFromKey())
	cancel()
	if err != nil {
		return nil, err
	}
	fmt.Printf(" GetDatatype %T \n", resp.Kvs)
	for k, v := range resp.Kvs {
		fmt.Printf("GetData k %v, v %v\n", k, v)
	}
	return resp, err
}

func (con *DBServer) GetMarshaled(prefix string, columns []interface{})  (*[]map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	resp, err := con.cli.Get(ctx, prefix,clientv3.WithFromKey())
	cancel()
	if err != nil {
		return nil, err
	}
	retMaps := map[string]map[string]string{}
	columnsMap := map[string]bool{}
	for _, col := range columns {
		columnsMap[col.(string)] = true
	}
	fmt.Printf("GetMarshaled columnsMap = %+v\n", columnsMap)
	for _, v := range resp.Kvs {
		keys := strings.Split(string(v.Key), "/")
		len := len(keys)
		fmt.Printf("GetMarshaled col name = %s %s\n", keys[len-1], string(v.Key))
		if _, ok := columnsMap[keys[len-1]]; !ok {
			fmt.Printf("GetMarshaled NO\n")
			continue
		}
		valsmap, ok := retMaps[keys[len-3]]
		fmt.Printf("GetMarshaled valsmap=%v\n", valsmap)
		if !ok {
			valsmap = map[string]string{}
		}
		valsmap[keys[len-1]] = string(v.Value)
		fmt.Printf("GetMarshaled $v=%v\n", keys[len-1], string(v.Value))

		// TODO
		retMaps[keys[len-3]] = valsmap
	}
	values := []map[string]string{}
	for _, value := range retMaps {
		values = append(values, value)
	}
	return &values, nil
}

/*func Marshal(kv []*mvccpb.KeyValue) (*[]map[string]string, error) {
	retMaps := map[string]map[string]string{}
	for _, v := range kv {
		keys := strings.Split(string(v.Key), "/")
		len := len(keys)
		valsmap, ok := retMaps[keys[len-2]]
		if !ok {
			valsmap = map[string]string{}
		}
		valsmap[keys[len-1]] = string(v.Value)
		// TODO
		retMaps[keys[len-2]] = valsmap
	}
	values := []map[string]string{}
	for _, value := range retMaps {
		values = append(values, value)
	}
	return &values, nil
}*/