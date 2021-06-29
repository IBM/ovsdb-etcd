package OVN_Southbound

import "github.com/ibm/ovsdb-etcd/pkg/libovsdb"

type Address_Set struct {
	Addresses libovsdb.OvsSet `json:"addresses,omitempty"`
	Name      string          `json:"name,omitempty"`
	Version   libovsdb.UUID   `json:"_version,omitempty"`
	Uuid      libovsdb.UUID   `json:"_uuid,omitempty"`
}

type BFD struct {
	Detect_mult  int64           `json:"detect_mult,omitempty"`
	Disc         int64           `json:"disc,omitempty"`
	Dst_ip       string          `json:"dst_ip,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Logical_port string          `json:"logical_port,omitempty"`
	Min_rx       int64           `json:"min_rx,omitempty"`
	Min_tx       int64           `json:"min_tx,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Src_port     int64           `json:"src_port,omitempty"`
	Status       string          `json:"status,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Chassis struct {
	Encaps                libovsdb.OvsSet `json:"encaps,omitempty"`
	External_ids          libovsdb.OvsMap `json:"external_ids,omitempty"`
	Hostname              string          `json:"hostname,omitempty"`
	Name                  string          `json:"name,omitempty"`
	Nb_cfg                int64           `json:"nb_cfg,omitempty"`
	Other_config          libovsdb.OvsMap `json:"other_config,omitempty"`
	Transport_zones       libovsdb.OvsSet `json:"transport_zones,omitempty"`
	Vtep_logical_switches libovsdb.OvsSet `json:"vtep_logical_switches,omitempty"`
	Version               libovsdb.UUID   `json:"_version,omitempty"`
	Uuid                  libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Chassis_Private struct {
	Chassis          libovsdb.OvsSet `json:"chassis,omitempty"`
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Name             string          `json:"name,omitempty"`
	Nb_cfg           int64           `json:"nb_cfg,omitempty"`
	Nb_cfg_timestamp int64           `json:"nb_cfg_timestamp,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Connection struct {
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Inactivity_probe libovsdb.OvsSet `json:"inactivity_probe,omitempty"`
	Is_connected     bool            `json:"is_connected,omitempty"`
	Max_backoff      libovsdb.OvsSet `json:"max_backoff,omitempty"`
	Other_config     libovsdb.OvsMap `json:"other_config,omitempty"`
	Read_only        bool            `json:"read_only,omitempty"`
	Role             string          `json:"role,omitempty"`
	Status           libovsdb.OvsMap `json:"status,omitempty"`
	Target           string          `json:"target,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Controller_Event struct {
	Chassis    libovsdb.OvsSet `json:"chassis,omitempty"`
	Event_info libovsdb.OvsMap `json:"event_info,omitempty"`
	Event_type string          `json:"event_type,omitempty"`
	Seq_num    int64           `json:"seq_num,omitempty"`
	Version    libovsdb.UUID   `json:"_version,omitempty"`
	Uuid       libovsdb.UUID   `json:"_uuid,omitempty"`
}

type DHCP_Options struct {
	Code    int64         `json:"code,omitempty"`
	Name    string        `json:"name,omitempty"`
	Type    string        `json:"type,omitempty"`
	Version libovsdb.UUID `json:"_version,omitempty"`
	Uuid    libovsdb.UUID `json:"_uuid,omitempty"`
}

type DHCPv6_Options struct {
	Code    int64         `json:"code,omitempty"`
	Name    string        `json:"name,omitempty"`
	Type    string        `json:"type,omitempty"`
	Version libovsdb.UUID `json:"_version,omitempty"`
	Uuid    libovsdb.UUID `json:"_uuid,omitempty"`
}

type DNS struct {
	Datapaths    libovsdb.OvsSet `json:"datapaths,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Records      libovsdb.OvsMap `json:"records,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Datapath_Binding struct {
	External_ids   libovsdb.OvsMap `json:"external_ids,omitempty"`
	Load_balancers libovsdb.OvsSet `json:"load_balancers,omitempty"`
	Tunnel_key     int64           `json:"tunnel_key,omitempty"`
	Version        libovsdb.UUID   `json:"_version,omitempty"`
	Uuid           libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Encap struct {
	Chassis_name string          `json:"chassis_name,omitempty"`
	Ip           string          `json:"ip,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Type         string          `json:"type,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type FDB struct {
	Dp_key   int64         `json:"dp_key,omitempty"`
	Mac      string        `json:"mac,omitempty"`
	Port_key int64         `json:"port_key,omitempty"`
	Version  libovsdb.UUID `json:"_version,omitempty"`
	Uuid     libovsdb.UUID `json:"_uuid,omitempty"`
}

type Gateway_Chassis struct {
	Chassis      libovsdb.OvsSet `json:"chassis,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Name         string          `json:"name,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Priority     int64           `json:"priority,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type HA_Chassis struct {
	Chassis      libovsdb.OvsSet `json:"chassis,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Priority     int64           `json:"priority,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type HA_Chassis_Group struct {
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Ha_chassis   libovsdb.OvsSet `json:"ha_chassis,omitempty"`
	Name         string          `json:"name,omitempty"`
	Ref_chassis  libovsdb.OvsSet `json:"ref_chassis,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type IGMP_Group struct {
	Address  string          `json:"address,omitempty"`
	Chassis  libovsdb.OvsSet `json:"chassis,omitempty"`
	Datapath libovsdb.OvsSet `json:"datapath,omitempty"`
	Ports    libovsdb.OvsSet `json:"ports,omitempty"`
	Version  libovsdb.UUID   `json:"_version,omitempty"`
	Uuid     libovsdb.UUID   `json:"_uuid,omitempty"`
}

type IP_Multicast struct {
	Datapath       libovsdb.UUID   `json:"datapath,omitempty"`
	Enabled        libovsdb.OvsSet `json:"enabled,omitempty"`
	Eth_src        string          `json:"eth_src,omitempty"`
	Idle_timeout   libovsdb.OvsSet `json:"idle_timeout,omitempty"`
	Ip4_src        string          `json:"ip4_src,omitempty"`
	Ip6_src        string          `json:"ip6_src,omitempty"`
	Querier        libovsdb.OvsSet `json:"querier,omitempty"`
	Query_interval libovsdb.OvsSet `json:"query_interval,omitempty"`
	Query_max_resp libovsdb.OvsSet `json:"query_max_resp,omitempty"`
	Seq_no         int64           `json:"seq_no,omitempty"`
	Table_size     libovsdb.OvsSet `json:"table_size,omitempty"`
	Version        libovsdb.UUID   `json:"_version,omitempty"`
	Uuid           libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Load_Balancer struct {
	Datapaths    libovsdb.OvsSet `json:"datapaths,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Name         string          `json:"name,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Protocol     libovsdb.OvsSet `json:"protocol,omitempty"`
	Vips         libovsdb.OvsMap `json:"vips,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_DP_Group struct {
	Datapaths libovsdb.OvsSet `json:"datapaths,omitempty"`
	Version   libovsdb.UUID   `json:"_version,omitempty"`
	Uuid      libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_Flow struct {
	Actions          string          `json:"actions,omitempty"`
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Logical_datapath libovsdb.OvsSet `json:"logical_datapath,omitempty"`
	Logical_dp_group libovsdb.OvsSet `json:"logical_dp_group,omitempty"`
	Match            string          `json:"match,omitempty"`
	Pipeline         string          `json:"pipeline,omitempty"`
	Priority         int64           `json:"priority,omitempty"`
	Table_id         int64           `json:"table_id,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type MAC_Binding struct {
	Datapath     libovsdb.UUID `json:"datapath,omitempty"`
	Ip           string        `json:"ip,omitempty"`
	Logical_port string        `json:"logical_port,omitempty"`
	Mac          string        `json:"mac,omitempty"`
	Version      libovsdb.UUID `json:"_version,omitempty"`
	Uuid         libovsdb.UUID `json:"_uuid,omitempty"`
}

type Meter struct {
	Bands   libovsdb.OvsSet `json:"bands,omitempty"`
	Name    string          `json:"name,omitempty"`
	Unit    string          `json:"unit,omitempty"`
	Version libovsdb.UUID   `json:"_version,omitempty"`
	Uuid    libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Meter_Band struct {
	Action     string        `json:"action,omitempty"`
	Burst_size int64         `json:"burst_size,omitempty"`
	Rate       int64         `json:"rate,omitempty"`
	Version    libovsdb.UUID `json:"_version,omitempty"`
	Uuid       libovsdb.UUID `json:"_uuid,omitempty"`
}

type Multicast_Group struct {
	Datapath   libovsdb.UUID   `json:"datapath,omitempty"`
	Name       string          `json:"name,omitempty"`
	Ports      libovsdb.OvsSet `json:"ports,omitempty"`
	Tunnel_key int64           `json:"tunnel_key,omitempty"`
	Version    libovsdb.UUID   `json:"_version,omitempty"`
	Uuid       libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Port_Binding struct {
	Chassis          libovsdb.OvsSet `json:"chassis,omitempty"`
	Datapath         libovsdb.UUID   `json:"datapath,omitempty"`
	Encap            libovsdb.OvsSet `json:"encap,omitempty"`
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Gateway_chassis  libovsdb.OvsSet `json:"gateway_chassis,omitempty"`
	Ha_chassis_group libovsdb.OvsSet `json:"ha_chassis_group,omitempty"`
	Logical_port     string          `json:"logical_port,omitempty"`
	Mac              libovsdb.OvsSet `json:"mac,omitempty"`
	Nat_addresses    libovsdb.OvsSet `json:"nat_addresses,omitempty"`
	Options          libovsdb.OvsMap `json:"options,omitempty"`
	Parent_port      libovsdb.OvsSet `json:"parent_port,omitempty"`
	Tag              libovsdb.OvsSet `json:"tag,omitempty"`
	Tunnel_key       int64           `json:"tunnel_key,omitempty"`
	Type             string          `json:"type,omitempty"`
	Up               libovsdb.OvsSet `json:"up,omitempty"`
	Virtual_parent   libovsdb.OvsSet `json:"virtual_parent,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Port_Group struct {
	Name    string          `json:"name,omitempty"`
	Ports   libovsdb.OvsSet `json:"ports,omitempty"`
	Version libovsdb.UUID   `json:"_version,omitempty"`
	Uuid    libovsdb.UUID   `json:"_uuid,omitempty"`
}

type RBAC_Permission struct {
	Authorization libovsdb.OvsSet `json:"authorization,omitempty"`
	Insert_delete bool            `json:"insert_delete,omitempty"`
	Table         string          `json:"table,omitempty"`
	Update        libovsdb.OvsSet `json:"update,omitempty"`
	Version       libovsdb.UUID   `json:"_version,omitempty"`
	Uuid          libovsdb.UUID   `json:"_uuid,omitempty"`
}

type RBAC_Role struct {
	Name        string          `json:"name,omitempty"`
	Permissions libovsdb.OvsMap `json:"permissions,omitempty"`
	Version     libovsdb.UUID   `json:"_version,omitempty"`
	Uuid        libovsdb.UUID   `json:"_uuid,omitempty"`
}

type SB_Global struct {
	Connections  libovsdb.OvsSet `json:"connections,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Ipsec        bool            `json:"ipsec,omitempty"`
	Nb_cfg       int64           `json:"nb_cfg,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Ssl          libovsdb.OvsSet `json:"ssl,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type SSL struct {
	Bootstrap_ca_cert bool            `json:"bootstrap_ca_cert,omitempty"`
	Ca_cert           string          `json:"ca_cert,omitempty"`
	Certificate       string          `json:"certificate,omitempty"`
	External_ids      libovsdb.OvsMap `json:"external_ids,omitempty"`
	Private_key       string          `json:"private_key,omitempty"`
	Ssl_ciphers       string          `json:"ssl_ciphers,omitempty"`
	Ssl_protocols     string          `json:"ssl_protocols,omitempty"`
	Version           libovsdb.UUID   `json:"_version,omitempty"`
	Uuid              libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Service_Monitor struct {
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Ip           string          `json:"ip,omitempty"`
	Logical_port string          `json:"logical_port,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Port         int64           `json:"port,omitempty"`
	Protocol     libovsdb.OvsSet `json:"protocol,omitempty"`
	Src_ip       string          `json:"src_ip,omitempty"`
	Src_mac      string          `json:"src_mac,omitempty"`
	Status       libovsdb.OvsSet `json:"status,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}
