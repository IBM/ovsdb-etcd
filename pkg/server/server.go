package main

import (
	"encoding/json"
	//	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"
	//	"time"
	//	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/roytman/ovsdb-etcd/pkg/ovsdb"
)


/*type Listener int
type Reply struct  {
	Data string
}

var schemas map[string]string*/


//const schemasNB := '{"name":"OVN_Northbound","version":"5.30.0","cksum":"3273824429 27172","tables":{"NB_Global":{"columns":{"name":{"type":"string"},"nb_cfg":{"type":{"key":"integer"}},"nb_cfg_timestamp":{"type":{"key":"integer"}},"sb_cfg":{"type":{"key":"integer"}},"sb_cfg_timestamp":{"type":{"key":"integer"}},"hv_cfg":{"type":{"key":"integer"}},"hv_cfg_timestamp":{"type":{"key":"integer"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"connections":{"type":{"key":{"type":"uuid","refTable":"Connection"},"min":0,"max":"unlimited"}},"ssl":{"type":{"key":{"type":"uuid","refTable":"SSL"},"min":0,"max":1}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"ipsec":{"type":"boolean"}},"maxRows":1,"isRoot":true},"Logical_Switch":{"columns":{"name":{"type":"string"},"ports":{"type":{"key":{"type":"uuid","refTable":"Logical_Switch_Port","refType":"strong"},"min":0,"max":"unlimited"}},"acls":{"type":{"key":{"type":"uuid","refTable":"ACL","refType":"strong"},"min":0,"max":"unlimited"}},"qos_rules":{"type":{"key":{"type":"uuid","refTable":"QoS","refType":"strong"},"min":0,"max":"unlimited"}},"load_balancer":{"type":{"key":{"type":"uuid","refTable":"Load_Balancer","refType":"weak"},"min":0,"max":"unlimited"}},"dns_records":{"type":{"key":{"type":"uuid","refTable":"DNS","refType":"weak"},"min":0,"max":"unlimited"}},"other_config":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"forwarding_groups":{"type":{"key":{"type":"uuid","refTable":"Forwarding_Group","refType":"strong"},"min":0,"max":"unlimited"}}},"isRoot":true},"Logical_Switch_Port":{"columns":{"name":{"type":"string"},"type":{"type":"string"},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"parent_name":{"type":{"key":"string","min":0,"max":1}},"tag_request":{"type":{"key":{"type":"integer","minInteger":0,"maxInteger":4095},"min":0,"max":1}},"tag":{"type":{"key":{"type":"integer","minInteger":1,"maxInteger":4095},"min":0,"max":1}},"addresses":{"type":{"key":"string","min":0,"max":"unlimited"}},"dynamic_addresses":{"type":{"key":"string","min":0,"max":1}},"port_security":{"type":{"key":"string","min":0,"max":"unlimited"}},"up":{"type":{"key":"boolean","min":0,"max":1}},"enabled":{"type":{"key":"boolean","min":0,"max":1}},"dhcpv4_options":{"type":{"key":{"type":"uuid","refTable":"DHCP_Options","refType":"weak"},"min":0,"max":1}},"dhcpv6_options":{"type":{"key":{"type":"uuid","refTable":"DHCP_Options","refType":"weak"},"min":0,"max":1}},"ha_chassis_group":{"type":{"key":{"type":"uuid","refTable":"HA_Chassis_Group","refType":"strong"},"min":0,"max":1}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"indexes":[["name"]],"isRoot":false},"Forwarding_Group":{"columns":{"name":{"type":"string"},"vip":{"type":"string"},"vmac":{"type":"string"},"liveness":{"type":"boolean"},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"child_port":{"type":{"key":"string","min":1,"max":"unlimited"}}},"isRoot":false},"Address_Set":{"columns":{"name":{"type":"string"},"addresses":{"type":{"key":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"indexes":[["name"]],"isRoot":true},"Port_Group":{"columns":{"name":{"type":"string"},"ports":{"type":{"key":{"type":"uuid","refTable":"Logical_Switch_Port","refType":"weak"},"min":0,"max":"unlimited"}},"acls":{"type":{"key":{"type":"uuid","refTable":"ACL","refType":"strong"},"min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"indexes":[["name"]],"isRoot":true},"Load_Balancer":{"columns":{"name":{"type":"string"},"vips":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"protocol":{"type":{"key":{"type":"string","enum":["set",["tcp","udp","sctp"]]},"min":0,"max":1}},"health_check":{"type":{"key":{"type":"uuid","refTable":"Load_Balancer_Health_Check","refType":"strong"},"min":0,"max":"unlimited"}},"ip_port_mappings":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"selection_fields":{"type":{"key":{"type":"string","enum":["set",["eth_src","eth_dst","ip_src","ip_dst","tp_src","tp_dst"]]},"min":0,"max":"unlimited"}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":true},"Load_Balancer_Health_Check":{"columns":{"vip":{"type":"string"},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"ACL":{"columns":{"name":{"type":{"key":{"type":"string","maxLength":63},"min":0,"max":1}},"priority":{"type":{"key":{"type":"integer","minInteger":0,"maxInteger":32767}}},"direction":{"type":{"key":{"type":"string","enum":["set",["from-lport","to-lport"]]}}},"match":{"type":"string"},"action":{"type":{"key":{"type":"string","enum":["set",["allow","allow-related","drop","reject"]]}}},"log":{"type":"boolean"},"severity":{"type":{"key":{"type":"string","enum":["set",["alert","warning","notice","info","debug"]]},"min":0,"max":1}},"meter":{"type":{"key":"string","min":0,"max":1}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"QoS":{"columns":{"priority":{"type":{"key":{"type":"integer","minInteger":0,"maxInteger":32767}}},"direction":{"type":{"key":{"type":"string","enum":["set",["from-lport","to-lport"]]}}},"match":{"type":"string"},"action":{"type":{"key":{"type":"string","enum":["set",["dscp"]]},"value":{"type":"integer","minInteger":0,"maxInteger":63},"min":0,"max":"unlimited"}},"bandwidth":{"type":{"key":{"type":"string","enum":["set",["rate","burst"]]},"value":{"type":"integer","minInteger":1,"maxInteger":4294967295},"min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"Meter":{"columns":{"name":{"type":"string"},"unit":{"type":{"key":{"type":"string","enum":["set",["kbps","pktps"]]}}},"bands":{"type":{"key":{"type":"uuid","refTable":"Meter_Band","refType":"strong"},"min":1,"max":"unlimited"}},"fair":{"type":{"key":"boolean","min":0,"max":1}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"indexes":[["name"]],"isRoot":true},"Meter_Band":{"columns":{"action":{"type":{"key":{"type":"string","enum":["set",["drop"]]}}},"rate":{"type":{"key":{"type":"integer","minInteger":1,"maxInteger":4294967295}}},"burst_size":{"type":{"key":{"type":"integer","minInteger":0,"maxInteger":4294967295}}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"Logical_Router":{"columns":{"name":{"type":"string"},"ports":{"type":{"key":{"type":"uuid","refTable":"Logical_Router_Port","refType":"strong"},"min":0,"max":"unlimited"}},"static_routes":{"type":{"key":{"type":"uuid","refTable":"Logical_Router_Static_Route","refType":"strong"},"min":0,"max":"unlimited"}},"policies":{"type":{"key":{"type":"uuid","refTable":"Logical_Router_Policy","refType":"strong"},"min":0,"max":"unlimited"}},"enabled":{"type":{"key":"boolean","min":0,"max":1}},"nat":{"type":{"key":{"type":"uuid","refTable":"NAT","refType":"strong"},"min":0,"max":"unlimited"}},"load_balancer":{"type":{"key":{"type":"uuid","refTable":"Load_Balancer","refType":"weak"},"min":0,"max":"unlimited"}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":true},"Logical_Router_Port":{"columns":{"name":{"type":"string"},"gateway_chassis":{"type":{"key":{"type":"uuid","refTable":"Gateway_Chassis","refType":"strong"},"min":0,"max":"unlimited"}},"ha_chassis_group":{"type":{"key":{"type":"uuid","refTable":"HA_Chassis_Group","refType":"strong"},"min":0,"max":1}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"networks":{"type":{"key":"string","min":1,"max":"unlimited"}},"mac":{"type":"string"},"peer":{"type":{"key":"string","min":0,"max":1}},"enabled":{"type":{"key":"boolean","min":0,"max":1}},"ipv6_ra_configs":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"ipv6_prefix":{"type":{"key":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"indexes":[["name"]],"isRoot":false},"Logical_Router_Static_Route":{"columns":{"ip_prefix":{"type":"string"},"policy":{"type":{"key":{"type":"string","enum":["set",["src-ip","dst-ip"]]},"min":0,"max":1}},"nexthop":{"type":"string"},"output_port":{"type":{"key":"string","min":0,"max":1}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"Logical_Router_Policy":{"columns":{"priority":{"type":{"key":{"type":"integer","minInteger":0,"maxInteger":32767}}},"match":{"type":"string"},"action":{"type":{"key":{"type":"string","enum":["set",["allow","drop","reroute"]]}}},"nexthop":{"type":{"key":"string","min":0,"max":1}},"nexthops":{"type":{"key":"string","min":0,"max":"unlimited"}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"NAT":{"columns":{"external_ip":{"type":"string"},"external_mac":{"type":{"key":"string","min":0,"max":1}},"external_port_range":{"type":"string"},"logical_ip":{"type":"string"},"logical_port":{"type":{"key":"string","min":0,"max":1}},"type":{"type":{"key":{"type":"string","enum":["set",["dnat","snat","dnat_and_snat"]]}}},"allowed_ext_ips":{"type":{"key":{"type":"uuid","refTable":"Address_Set","refType":"strong"},"min":0,"max":1}},"exempted_ext_ips":{"type":{"key":{"type":"uuid","refTable":"Address_Set","refType":"strong"},"min":0,"max":1}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"DHCP_Options":{"columns":{"cidr":{"type":"string"},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":true},"Connection":{"columns":{"target":{"type":"string"},"max_backoff":{"type":{"key":{"type":"integer","minInteger":1000},"min":0,"max":1}},"inactivity_probe":{"type":{"key":"integer","min":0,"max":1}},"other_config":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"is_connected":{"type":"boolean","ephemeral":true},"status":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"},"ephemeral":true}},"indexes":[["target"]]},"DNS":{"columns":{"records":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":true},"SSL":{"columns":{"private_key":{"type":"string"},"certificate":{"type":"string"},"ca_cert":{"type":"string"},"bootstrap_ca_cert":{"type":"boolean"},"ssl_protocols":{"type":"string"},"ssl_ciphers":{"type":"string"},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"maxRows":1},"Gateway_Chassis":{"columns":{"name":{"type":"string"},"chassis_name":{"type":"string"},"priority":{"type":{"key":{"type":"integer","minInteger":0,"maxInteger":32767}}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}},"options":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"indexes":[["name"]],"isRoot":false},"HA_Chassis":{"columns":{"chassis_name":{"type":"string"},"priority":{"type":{"key":{"type":"integer","minInteger":0,"maxInteger":32767}}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"isRoot":false},"HA_Chassis_Group":{"columns":{"name":{"type":"string"},"ha_chassis":{"type":{"key":{"type":"uuid","refTable":"HA_Chassis","refType":"strong"},"min":0,"max":"unlimited"}},"external_ids":{"type":{"key":"string","value":"string","min":0,"max":"unlimited"}}},"indexes":[["name"]],"isRoot":true}}}'
//const schemaS = "{\"name\":\"_Server\",\"version\":\"1.1.0\",\"cksum\":\"3236486585 698\",\"tables\":{\"Database\":{\"columns\":{\"name\":{\"type\":\"string\"},\"model\":{\"type\":{\"key\":{\"type\":\"string\",\"enum\":[\"set\",[\"standalone\",\"clustered\"]]}}},\"connected\":{\"type\":\"boolean\"},\"leader\":{\"type\":\"boolean\"},\"schema\":{\"type\":{\"key\":{\"type\":\"string\"},\"min\":0,\"max\":1}},\"cid\":{\"type\":{\"key\":{\"type\":\"uuid\"},\"min\":0,\"max\":1}},\"sid\":{\"type\":{\"key\":{\"type\":\"uuid\"},\"min\":0,\"max\":1}},\"index\":{\"type\":{\"key\":{\"type\":\"integer\"},\"min\":0,\"max\":1}}},\"isRoot\":true}}"


/*func (l *Listener) GetLine(line []byte, reply *string) error {
	rv := string(line)
	fmt.Printf("Receive: %v\n", rv)
	//*reply = Reply{rv}
	return nil
}


func (l *Listener) Get_schema(line string, reply *interface{}) error {
	fmt.Printf("Receive: %v\n", line)
	var f interface{}
	err := json.Unmarshal([]byte(schemas[line]), &f)
	if err != nil {
		fmt.Printf("Error %v\n", err)
	}
	*reply = f
	fmt.Printf("%v\n", *reply)
	return nil
}*/

//var cond string

/*func (l *Listener) Monitor_cond(line string, reply *string) error {
	fmt.Printf("Receive: %v\n", line)
	*reply = schemas[line]
	return nil
}*/


//
//func (l *Listener) List_dbs(line string, reply *Reply) error {
//	fmt.Printf("Receive: %v\n", rv)
//	*reply = Reply{rv}
//	return nil
//}

/*type myCodec struct {
	code rpc.ServerCodec
}

func (c *myCodec) ReadRequestHeader(r *rpc.Request) error {
	 e := c.code.ReadRequestHeader(r)
	 if e!= nil {
	 	return e
	 }
	 fmt.Printf("Reqyest = %+v\n", r)
	dot := strings.LastIndex(r.ServiceMethod, ".")
	needClass := false
	if dot < 0 {
		needClass = true
	}
	methodName := r.ServiceMethod[dot+1:]
	startLetter := methodName[:1]
	startLetter = strings.ToUpper(startLetter)
	methodName = startLetter + methodName[1:]
	if needClass {
		r.ServiceMethod = "ovsdb." + methodName
	}  else {
		r.ServiceMethod = r.ServiceMethod[:dot] + methodName
	}
	return e
}

func (c *myCodec) ReadRequestBody(x interface{}) error {
	return c.code.ReadRequestBody(x)
}

func (c *myCodec) WriteResponse(r *rpc.Response, x interface{}) error {
	return c.code.WriteResponse(r, x)
}

func (c *myCodec) Close() error {
	return c.code.Close()
}

func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &myCodec{ code: jsonrpc.NewServerCodec(conn)}
}
*/
func main() {
	ovsdb := ovsdb.NewService()

	err := ovsdb.AddSchema("_Server", "./json/_server.ovsschema")
	if err != nil {
		log.Fatal(err)
	}
	err = ovsdb.AddSchema("OVN_Northbound", "./json/ovn-nb.ovsschema")
	if err != nil {
		log.Fatal(err)
	}

	/*
	data, err := ioutil.ReadFile("./pkg/server/cond.json")
	if err != nil {
		log.Fatal(err)
	}
	*/

	//cond = string(data)
	/*var cli *clientv3.Client
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	fmt.Printf("etcd client is connected")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = cli.Put(ctx, "sample_key", "sample_value")
	cancel()
	if err != nil {
		log.Fatal(err)
	}*/


	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:12345")
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}
	/*inbound2, err := net.Listen("unix", "/tmp/echo.sock")
	if err != nil {
		log.Fatal(err)
	}
	defer inbound2.Close()*/

	///listener := new(Listener)
	ms := rpc.NewServer()

	ms.RegisterName("ovsdb", ovsdb)
	//ms.RegisterName("", listener)


	//rpc.RegisterName("", listener)
	/*go func() {
		for {
			conn, err := inbound2.Accept()
			if err != nil {
				continue
			}
			ms.ServeCodec(NewServerCodec(conn))
		}
    }()*/
	for {
		conn, err := inbound.Accept()
		if err != nil {
			continue
		}
		ms.ServeCodec(ovsdb.NewCodec(conn))
		//jsonrpc.ServeConn(conn)
	}
}