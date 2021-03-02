package OVN_Southbound

import "github.com/roytman/ovsdb-etcd/pkg/json"

type Load_Balancer struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Name         string            `json:"name,omitempty"`
	Vips         map[string]string `json:"vips,omitempty"`
	Protocol     string            `json:"protocol,omitempty"`
	Datapaths    []json.Uuid       `json:"datapaths,omitempty"`
}

type Logical_Flow struct {
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Logical_datapath json.Uuid         `json:"logical_datapath,omitempty"`
	Logical_dp_group json.Uuid         `json:"logical_dp_group,omitempty"`
	Pipeline         string            `json:"pipeline,omitempty"`
	Table_id         int64             `json:"table_id,omitempty"`
	Priority         int64             `json:"priority,omitempty"`
	Match            string            `json:"match,omitempty"`
	Actions          string            `json:"actions,omitempty"`
}

type Datapath_Binding struct {
	Tunnel_key     int64             `json:"tunnel_key,omitempty"`
	Load_balancers []json.Uuid       `json:"load_balancers,omitempty"`
	External_ids   map[string]string `json:"external_ids,omitempty"`
}

type Multicast_Group struct {
	Tunnel_key int64       `json:"tunnel_key,omitempty"`
	Ports      []json.Uuid `json:"ports,omitempty"`
	Datapath   json.Uuid   `json:"datapath,omitempty"`
	Name       string      `json:"name,omitempty"`
}

type DHCP_Options struct {
	Name string `json:"name,omitempty"`
	Code int64  `json:"code,omitempty"`
	Type string `json:"type,omitempty"`
}

type Chassis_Private struct {
	Name             string            `json:"name,omitempty"`
	Chassis          json.Uuid         `json:"chassis,omitempty"`
	Nb_cfg           int64             `json:"nb_cfg,omitempty"`
	Nb_cfg_timestamp int64             `json:"nb_cfg_timestamp,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
}

type RBAC_Role struct {
	Name        string               `json:"name,omitempty"`
	Permissions map[string]json.Uuid `json:"permissions,omitempty"`
}

type Gateway_Chassis struct {
	Chassis      json.Uuid         `json:"chassis,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Name         string            `json:"name,omitempty"`
}

type DHCPv6_Options struct {
	Name string `json:"name,omitempty"`
	Code int64  `json:"code,omitempty"`
	Type string `json:"type,omitempty"`
}

type Address_Set struct {
	Name      string   `json:"name,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
}

type IP_Multicast struct {
	Datapath       json.Uuid `json:"datapath,omitempty"`
	Querier        bool      `json:"querier,omitempty"`
	Idle_timeout   int64     `json:"idle_timeout,omitempty"`
	Query_max_resp int64     `json:"query_max_resp,omitempty"`
	Seq_no         int64     `json:"seq_no,omitempty"`
	Enabled        bool      `json:"enabled,omitempty"`
	Eth_src        string    `json:"eth_src,omitempty"`
	Ip4_src        string    `json:"ip4_src,omitempty"`
	Ip6_src        string    `json:"ip6_src,omitempty"`
	Table_size     int64     `json:"table_size,omitempty"`
	Query_interval int64     `json:"query_interval,omitempty"`
}

type Service_Monitor struct {
	Ip           string            `json:"ip,omitempty"`
	Protocol     string            `json:"protocol,omitempty"`
	Src_ip       string            `json:"src_ip,omitempty"`
	Status       string            `json:"status,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Port         int64             `json:"port,omitempty"`
	Logical_port string            `json:"logical_port,omitempty"`
	Src_mac      string            `json:"src_mac,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
}

type Meter_Band struct {
	Action     string `json:"action,omitempty"`
	Rate       int64  `json:"rate,omitempty"`
	Burst_size int64  `json:"burst_size,omitempty"`
}

type Encap struct {
	Options      map[string]string `json:"options,omitempty"`
	Ip           string            `json:"ip,omitempty"`
	Chassis_name string            `json:"chassis_name,omitempty"`
	Type         string            `json:"type,omitempty"`
}

type HA_Chassis struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Chassis      json.Uuid         `json:"chassis,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
}

type SSL struct {
	Certificate       string            `json:"certificate,omitempty"`
	Ca_cert           string            `json:"ca_cert,omitempty"`
	Bootstrap_ca_cert bool              `json:"bootstrap_ca_cert,omitempty"`
	Ssl_protocols     string            `json:"ssl_protocols,omitempty"`
	Ssl_ciphers       string            `json:"ssl_ciphers,omitempty"`
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Private_key       string            `json:"private_key,omitempty"`
}

type SB_Global struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Connections  []json.Uuid       `json:"connections,omitempty"`
	Ssl          json.Uuid         `json:"ssl,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Ipsec        bool              `json:"ipsec,omitempty"`
	Nb_cfg       int64             `json:"nb_cfg,omitempty"`
}

type HA_Chassis_Group struct {
	Name         string            `json:"name,omitempty"`
	Ha_chassis   []json.Uuid       `json:"ha_chassis,omitempty"`
	Ref_chassis  []json.Uuid       `json:"ref_chassis,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type Logical_DP_Group struct {
	Datapaths []json.Uuid `json:"datapaths,omitempty"`
}

type Connection struct {
	Other_config     map[string]string `json:"other_config,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Is_connected     bool              `json:"is_connected,omitempty"`
	Target           string            `json:"target,omitempty"`
	Max_backoff      int64             `json:"max_backoff,omitempty"`
	Inactivity_probe int64             `json:"inactivity_probe,omitempty"`
	Read_only        bool              `json:"read_only,omitempty"`
	Role             string            `json:"role,omitempty"`
	Status           map[string]string `json:"status,omitempty"`
}

type Chassis struct {
	Name                  string            `json:"name,omitempty"`
	Hostname              string            `json:"hostname,omitempty"`
	Encaps                []json.Uuid       `json:"encaps,omitempty"`
	Vtep_logical_switches []string          `json:"vtep_logical_switches,omitempty"`
	Nb_cfg                int64             `json:"nb_cfg,omitempty"`
	External_ids          map[string]string `json:"external_ids,omitempty"`
	Other_config          map[string]string `json:"other_config,omitempty"`
	Transport_zones       []string          `json:"transport_zones,omitempty"`
}

type MAC_Binding struct {
	Logical_port string    `json:"logical_port,omitempty"`
	Ip           string    `json:"ip,omitempty"`
	Mac          string    `json:"mac,omitempty"`
	Datapath     json.Uuid `json:"datapath,omitempty"`
}

type IGMP_Group struct {
	Address  string      `json:"address,omitempty"`
	Datapath json.Uuid   `json:"datapath,omitempty"`
	Chassis  json.Uuid   `json:"chassis,omitempty"`
	Ports    []json.Uuid `json:"ports,omitempty"`
}

type DNS struct {
	Records      map[string]string `json:"records,omitempty"`
	Datapaths    []json.Uuid       `json:"datapaths,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type Meter struct {
	Bands []json.Uuid `json:"bands,omitempty"`
	Name  string      `json:"name,omitempty"`
	Unit  string      `json:"unit,omitempty"`
}

type RBAC_Permission struct {
	Authorization []string `json:"authorization,omitempty"`
	Insert_delete bool     `json:"insert_delete,omitempty"`
	Update        []string `json:"update,omitempty"`
	Table         string   `json:"table,omitempty"`
}

type Port_Group struct {
	Name  string   `json:"name,omitempty"`
	Ports []string `json:"ports,omitempty"`
}

type Port_Binding struct {
	Logical_port     string            `json:"logical_port,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Nat_addresses    []string          `json:"nat_addresses,omitempty"`
	Ha_chassis_group json.Uuid         `json:"ha_chassis_group,omitempty"`
	Options          map[string]string `json:"options,omitempty"`
	Encap            json.Uuid         `json:"encap,omitempty"`
	Mac              []string          `json:"mac,omitempty"`
	Chassis          json.Uuid         `json:"chassis,omitempty"`
	Type             string            `json:"type,omitempty"`
	Parent_port      string            `json:"parent_port,omitempty"`
	Tag              int64             `json:"tag,omitempty"`
	Virtual_parent   string            `json:"virtual_parent,omitempty"`
	Gateway_chassis  []json.Uuid       `json:"gateway_chassis,omitempty"`
	Datapath         json.Uuid         `json:"datapath,omitempty"`
	Tunnel_key       int64             `json:"tunnel_key,omitempty"`
}

type Controller_Event struct {
	Seq_num    int64             `json:"seq_num,omitempty"`
	Event_type string            `json:"event_type,omitempty"`
	Event_info map[string]string `json:"event_info,omitempty"`
	Chassis    json.Uuid         `json:"chassis,omitempty"`
}
