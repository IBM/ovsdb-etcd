package OVN_Southbound

import "github.com/roytman/ovsdb-etcd/pkg/json"

type Load_Balancer struct {
	Name         string            `json:"name,omitempty"`
	Vips         map[string]string `json:"vips,omitempty"`
	Protocol     string            `json:"protocol,omitempty"`
	Datapaths    []json.Uuid       `json:"datapaths,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type HA_Chassis struct {
	Priority     int64             `json:"priority,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Chassis      json.Uuid         `json:"chassis,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Port_Binding struct {
	Options          map[string]string `json:"options,omitempty"`
	Chassis          json.Uuid         `json:"chassis,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Gateway_chassis  []json.Uuid       `json:"gateway_chassis,omitempty"`
	Encap            json.Uuid         `json:"encap,omitempty"`
	Ha_chassis_group json.Uuid         `json:"ha_chassis_group,omitempty"`
	Tunnel_key       int64             `json:"tunnel_key,omitempty"`
	Parent_port      string            `json:"parent_port,omitempty"`
	Tag              int64             `json:"tag,omitempty"`
	Virtual_parent   string            `json:"virtual_parent,omitempty"`
	Mac              []string          `json:"mac,omitempty"`
	Logical_port     string            `json:"logical_port,omitempty"`
	Type             string            `json:"type,omitempty"`
	Datapath         json.Uuid         `json:"datapath,omitempty"`
	Nat_addresses    []string          `json:"nat_addresses,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type Datapath_Binding struct {
	Tunnel_key     int64             `json:"tunnel_key,omitempty"`
	Load_balancers []json.Uuid       `json:"load_balancers,omitempty"`
	External_ids   map[string]string `json:"external_ids,omitempty"`
	Version        json.Uuid         `json:"_version,omitempty"`
	Uuid           json.Uuid         `json:"uuid,omitempty"`
}

type Meter struct {
	Name    string      `json:"name,omitempty"`
	Unit    string      `json:"unit,omitempty"`
	Bands   []json.Uuid `json:"bands,omitempty"`
	Version json.Uuid   `json:"_version,omitempty"`
	Uuid    json.Uuid   `json:"uuid,omitempty"`
}

type HA_Chassis_Group struct {
	Name         string            `json:"name,omitempty"`
	Ha_chassis   []json.Uuid       `json:"ha_chassis,omitempty"`
	Ref_chassis  []json.Uuid       `json:"ref_chassis,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Gateway_Chassis struct {
	Priority     int64             `json:"priority,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Name         string            `json:"name,omitempty"`
	Chassis      json.Uuid         `json:"chassis,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Connection struct {
	Is_connected     bool              `json:"is_connected,omitempty"`
	Status           map[string]string `json:"status,omitempty"`
	Other_config     map[string]string `json:"other_config,omitempty"`
	Max_backoff      int64             `json:"max_backoff,omitempty"`
	Inactivity_probe int64             `json:"inactivity_probe,omitempty"`
	Read_only        bool              `json:"read_only,omitempty"`
	Role             string            `json:"role,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Target           string            `json:"target,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type DHCPv6_Options struct {
	Name    string    `json:"name,omitempty"`
	Code    int64     `json:"code,omitempty"`
	Type    string    `json:"type,omitempty"`
	Version json.Uuid `json:"_version,omitempty"`
	Uuid    json.Uuid `json:"uuid,omitempty"`
}

type Chassis struct {
	Transport_zones       []string          `json:"transport_zones,omitempty"`
	Name                  string            `json:"name,omitempty"`
	Hostname              string            `json:"hostname,omitempty"`
	Encaps                []json.Uuid       `json:"encaps,omitempty"`
	Vtep_logical_switches []string          `json:"vtep_logical_switches,omitempty"`
	Nb_cfg                int64             `json:"nb_cfg,omitempty"`
	External_ids          map[string]string `json:"external_ids,omitempty"`
	Other_config          map[string]string `json:"other_config,omitempty"`
	Version               json.Uuid         `json:"_version,omitempty"`
	Uuid                  json.Uuid         `json:"uuid,omitempty"`
}

type Multicast_Group struct {
	Ports      []json.Uuid `json:"ports,omitempty"`
	Datapath   json.Uuid   `json:"datapath,omitempty"`
	Name       string      `json:"name,omitempty"`
	Tunnel_key int64       `json:"tunnel_key,omitempty"`
	Version    json.Uuid   `json:"_version,omitempty"`
	Uuid       json.Uuid   `json:"uuid,omitempty"`
}

type Logical_DP_Group struct {
	Datapaths []json.Uuid `json:"datapaths,omitempty"`
	Version   json.Uuid   `json:"_version,omitempty"`
	Uuid      json.Uuid   `json:"uuid,omitempty"`
}

type SSL struct {
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Private_key       string            `json:"private_key,omitempty"`
	Certificate       string            `json:"certificate,omitempty"`
	Ca_cert           string            `json:"ca_cert,omitempty"`
	Bootstrap_ca_cert bool              `json:"bootstrap_ca_cert,omitempty"`
	Ssl_protocols     string            `json:"ssl_protocols,omitempty"`
	Ssl_ciphers       string            `json:"ssl_ciphers,omitempty"`
	Version           json.Uuid         `json:"_version,omitempty"`
	Uuid              json.Uuid         `json:"uuid,omitempty"`
}

type MAC_Binding struct {
	Logical_port string    `json:"logical_port,omitempty"`
	Ip           string    `json:"ip,omitempty"`
	Mac          string    `json:"mac,omitempty"`
	Datapath     json.Uuid `json:"datapath,omitempty"`
	Version      json.Uuid `json:"_version,omitempty"`
	Uuid         json.Uuid `json:"uuid,omitempty"`
}

type Controller_Event struct {
	Event_info map[string]string `json:"event_info,omitempty"`
	Chassis    json.Uuid         `json:"chassis,omitempty"`
	Seq_num    int64             `json:"seq_num,omitempty"`
	Event_type string            `json:"event_type,omitempty"`
	Version    json.Uuid         `json:"_version,omitempty"`
	Uuid       json.Uuid         `json:"uuid,omitempty"`
}

type Logical_Flow struct {
	Pipeline         string            `json:"pipeline,omitempty"`
	Table_id         int64             `json:"table_id,omitempty"`
	Priority         int64             `json:"priority,omitempty"`
	Match            string            `json:"match,omitempty"`
	Actions          string            `json:"actions,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Logical_datapath json.Uuid         `json:"logical_datapath,omitempty"`
	Logical_dp_group json.Uuid         `json:"logical_dp_group,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type DNS struct {
	Datapaths    []json.Uuid       `json:"datapaths,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Records      map[string]string `json:"records,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Encap struct {
	Type         string            `json:"type,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Ip           string            `json:"ip,omitempty"`
	Chassis_name string            `json:"chassis_name,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Address_Set struct {
	Name      string    `json:"name,omitempty"`
	Addresses []string  `json:"addresses,omitempty"`
	Version   json.Uuid `json:"_version,omitempty"`
	Uuid      json.Uuid `json:"uuid,omitempty"`
}

type Service_Monitor struct {
	Src_ip       string            `json:"src_ip,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Ip           string            `json:"ip,omitempty"`
	Protocol     string            `json:"protocol,omitempty"`
	Logical_port string            `json:"logical_port,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Port         int64             `json:"port,omitempty"`
	Src_mac      string            `json:"src_mac,omitempty"`
	Status       string            `json:"status,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type IP_Multicast struct {
	Query_max_resp int64     `json:"query_max_resp,omitempty"`
	Datapath       json.Uuid `json:"datapath,omitempty"`
	Enabled        bool      `json:"enabled,omitempty"`
	Querier        bool      `json:"querier,omitempty"`
	Eth_src        string    `json:"eth_src,omitempty"`
	Idle_timeout   int64     `json:"idle_timeout,omitempty"`
	Ip4_src        string    `json:"ip4_src,omitempty"`
	Ip6_src        string    `json:"ip6_src,omitempty"`
	Table_size     int64     `json:"table_size,omitempty"`
	Query_interval int64     `json:"query_interval,omitempty"`
	Seq_no         int64     `json:"seq_no,omitempty"`
	Version        json.Uuid `json:"_version,omitempty"`
	Uuid           json.Uuid `json:"uuid,omitempty"`
}

type Port_Group struct {
	Name    string    `json:"name,omitempty"`
	Ports   []string  `json:"ports,omitempty"`
	Version json.Uuid `json:"_version,omitempty"`
	Uuid    json.Uuid `json:"uuid,omitempty"`
}

type Chassis_Private struct {
	Chassis          json.Uuid         `json:"chassis,omitempty"`
	Nb_cfg           int64             `json:"nb_cfg,omitempty"`
	Nb_cfg_timestamp int64             `json:"nb_cfg_timestamp,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Name             string            `json:"name,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type IGMP_Group struct {
	Address  string      `json:"address,omitempty"`
	Datapath json.Uuid   `json:"datapath,omitempty"`
	Chassis  json.Uuid   `json:"chassis,omitempty"`
	Ports    []json.Uuid `json:"ports,omitempty"`
	Version  json.Uuid   `json:"_version,omitempty"`
	Uuid     json.Uuid   `json:"uuid,omitempty"`
}

type RBAC_Role struct {
	Name        string               `json:"name,omitempty"`
	Permissions map[string]json.Uuid `json:"permissions,omitempty"`
	Version     json.Uuid            `json:"_version,omitempty"`
	Uuid        json.Uuid            `json:"uuid,omitempty"`
}

type SB_Global struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Connections  []json.Uuid       `json:"connections,omitempty"`
	Ssl          json.Uuid         `json:"ssl,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Ipsec        bool              `json:"ipsec,omitempty"`
	Nb_cfg       int64             `json:"nb_cfg,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Meter_Band struct {
	Action     string    `json:"action,omitempty"`
	Rate       int64     `json:"rate,omitempty"`
	Burst_size int64     `json:"burst_size,omitempty"`
	Version    json.Uuid `json:"_version,omitempty"`
	Uuid       json.Uuid `json:"uuid,omitempty"`
}

type DHCP_Options struct {
	Code    int64     `json:"code,omitempty"`
	Type    string    `json:"type,omitempty"`
	Name    string    `json:"name,omitempty"`
	Version json.Uuid `json:"_version,omitempty"`
	Uuid    json.Uuid `json:"uuid,omitempty"`
}

type RBAC_Permission struct {
	Insert_delete bool      `json:"insert_delete,omitempty"`
	Update        []string  `json:"update,omitempty"`
	Table         string    `json:"table,omitempty"`
	Authorization []string  `json:"authorization,omitempty"`
	Version       json.Uuid `json:"_version,omitempty"`
	Uuid          json.Uuid `json:"uuid,omitempty"`
}
