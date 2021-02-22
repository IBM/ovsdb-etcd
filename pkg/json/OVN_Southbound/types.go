package OVN_Southbound

import "github.com/roytman/ovsdb-etcd/pkg/json"

type DNS struct {
	Records      map[string]string `json:"records"`
	Datapaths    []json.Uuid       `json:"datapaths"`
	External_ids map[string]string `json:"external_ids"`
}

type Load_Balancer struct {
	Name         string            `json:"name"`
	Vips         map[string]string `json:"vips"`
	Protocol     string            `json:"protocol"`
	Datapaths    []json.Uuid       `json:"datapaths"`
	External_ids map[string]string `json:"external_ids"`
}

type Address_Set struct {
	Name      string   `json:"name"`
	Addresses []string `json:"addresses"`
}

type DHCP_Options struct {
	Code int64  `json:"code"`
	Type string `json:"type"`
	Name string `json:"name"`
}

type DHCPv6_Options struct {
	Name string `json:"name"`
	Code int64  `json:"code"`
	Type string `json:"type"`
}

type HA_Chassis_Group struct {
	Name         string            `json:"name"`
	Ha_chassis   []json.Uuid       `json:"ha_chassis"`
	Ref_chassis  []json.Uuid       `json:"ref_chassis"`
	External_ids map[string]string `json:"external_ids"`
}

type RBAC_Permission struct {
	Table         string   `json:"table"`
	Authorization []string `json:"authorization"`
	Insert_delete bool     `json:"insert_delete"`
	Update        []string `json:"update"`
}

type Meter_Band struct {
	Action     string `json:"action"`
	Rate       int64  `json:"rate"`
	Burst_size int64  `json:"burst_size"`
}

type Meter struct {
	Bands []json.Uuid `json:"bands"`
	Name  string      `json:"name"`
	Unit  string      `json:"unit"`
}

type Logical_DP_Group struct {
	Datapaths []json.Uuid `json:"datapaths"`
}

type Port_Group struct {
	Ports []string `json:"ports"`
	Name  string   `json:"name"`
}

type Service_Monitor struct {
	Options      map[string]string `json:"options"`
	Port         int64             `json:"port"`
	Logical_port string            `json:"logical_port"`
	Src_mac      string            `json:"src_mac"`
	Status       string            `json:"status"`
	External_ids map[string]string `json:"external_ids"`
	Ip           string            `json:"ip"`
	Protocol     string            `json:"protocol"`
	Src_ip       string            `json:"src_ip"`
}

type IP_Multicast struct {
	Querier        bool      `json:"querier"`
	Eth_src        string    `json:"eth_src"`
	Ip4_src        string    `json:"ip4_src"`
	Seq_no         int64     `json:"seq_no"`
	Query_interval int64     `json:"query_interval"`
	Query_max_resp int64     `json:"query_max_resp"`
	Datapath       json.Uuid `json:"datapath"`
	Enabled        bool      `json:"enabled"`
	Ip6_src        string    `json:"ip6_src"`
	Table_size     int64     `json:"table_size"`
	Idle_timeout   int64     `json:"idle_timeout"`
}

type IGMP_Group struct {
	Chassis  json.Uuid   `json:"chassis"`
	Ports    []json.Uuid `json:"ports"`
	Address  string      `json:"address"`
	Datapath json.Uuid   `json:"datapath"`
}

type RBAC_Role struct {
	Permissions map[string]json.Uuid `json:"permissions"`
	Name        string               `json:"name"`
}

type Chassis struct {
	Nb_cfg                int64             `json:"nb_cfg"`
	External_ids          map[string]string `json:"external_ids"`
	Other_config          map[string]string `json:"other_config"`
	Transport_zones       []string          `json:"transport_zones"`
	Name                  string            `json:"name"`
	Hostname              string            `json:"hostname"`
	Encaps                []json.Uuid       `json:"encaps"`
	Vtep_logical_switches []string          `json:"vtep_logical_switches"`
}

type MAC_Binding struct {
	Logical_port string    `json:"logical_port"`
	Ip           string    `json:"ip"`
	Mac          string    `json:"mac"`
	Datapath     json.Uuid `json:"datapath"`
}

type Gateway_Chassis struct {
	Name         string            `json:"name"`
	Chassis      json.Uuid         `json:"chassis"`
	Priority     int64             `json:"priority"`
	External_ids map[string]string `json:"external_ids"`
	Options      map[string]string `json:"options"`
}

type Port_Binding struct {
	Gateway_chassis  []json.Uuid       `json:"gateway_chassis"`
	Options          map[string]string `json:"options"`
	Chassis          json.Uuid         `json:"chassis"`
	Encap            json.Uuid         `json:"encap"`
	Nat_addresses    []string          `json:"nat_addresses"`
	Logical_port     string            `json:"logical_port"`
	Tunnel_key       int64             `json:"tunnel_key"`
	Parent_port      string            `json:"parent_port"`
	Tag              int64             `json:"tag"`
	Virtual_parent   string            `json:"virtual_parent"`
	Ha_chassis_group json.Uuid         `json:"ha_chassis_group"`
	Datapath         json.Uuid         `json:"datapath"`
	Mac              []string          `json:"mac"`
	Type             string            `json:"type"`
	External_ids     map[string]string `json:"external_ids"`
}

type Chassis_Private struct {
	External_ids     map[string]string `json:"external_ids"`
	Name             string            `json:"name"`
	Chassis          json.Uuid         `json:"chassis"`
	Nb_cfg           int64             `json:"nb_cfg"`
	Nb_cfg_timestamp int64             `json:"nb_cfg_timestamp"`
}

type Encap struct {
	Options      map[string]string `json:"options"`
	Ip           string            `json:"ip"`
	Chassis_name string            `json:"chassis_name"`
	Type         string            `json:"type"`
}

type HA_Chassis struct {
	Chassis      json.Uuid         `json:"chassis"`
	Priority     int64             `json:"priority"`
	External_ids map[string]string `json:"external_ids"`
}

type SSL struct {
	Certificate       string            `json:"certificate"`
	Ca_cert           string            `json:"ca_cert"`
	Bootstrap_ca_cert bool              `json:"bootstrap_ca_cert"`
	Ssl_protocols     string            `json:"ssl_protocols"`
	Ssl_ciphers       string            `json:"ssl_ciphers"`
	External_ids      map[string]string `json:"external_ids"`
	Private_key       string            `json:"private_key"`
}

type Multicast_Group struct {
	Datapath   json.Uuid   `json:"datapath"`
	Name       string      `json:"name"`
	Tunnel_key int64       `json:"tunnel_key"`
	Ports      []json.Uuid `json:"ports"`
}

type Datapath_Binding struct {
	Load_balancers []json.Uuid       `json:"load_balancers"`
	External_ids   map[string]string `json:"external_ids"`
	Tunnel_key     int64             `json:"tunnel_key"`
}

type Controller_Event struct {
	Chassis    json.Uuid         `json:"chassis"`
	Seq_num    int64             `json:"seq_num"`
	Event_type string            `json:"event_type"`
	Event_info map[string]string `json:"event_info"`
}

type Connection struct {
	Role             string            `json:"role"`
	Other_config     map[string]string `json:"other_config"`
	External_ids     map[string]string `json:"external_ids"`
	Inactivity_probe int64             `json:"inactivity_probe"`
	Max_backoff      int64             `json:"max_backoff"`
	Read_only        bool              `json:"read_only"`
	Is_connected     bool              `json:"is_connected"`
	Status           map[string]string `json:"status"`
	Target           string            `json:"target"`
}

type Logical_Flow struct {
	External_ids     map[string]string `json:"external_ids"`
	Logical_datapath json.Uuid         `json:"logical_datapath"`
	Logical_dp_group json.Uuid         `json:"logical_dp_group"`
	Pipeline         string            `json:"pipeline"`
	Table_id         int64             `json:"table_id"`
	Priority         int64             `json:"priority"`
	Match            string            `json:"match"`
	Actions          string            `json:"actions"`
}

type SB_Global struct {
	Nb_cfg       int64             `json:"nb_cfg"`
	External_ids map[string]string `json:"external_ids"`
	Connections  []json.Uuid       `json:"connections"`
	Ssl          json.Uuid         `json:"ssl"`
	Options      map[string]string `json:"options"`
	Ipsec        bool              `json:"ipsec"`
}
