package OVN_Northbound

import "github.com/ibm/ovsdb-etcd/pkg/libovsdb"

type ACL struct {
	Action       string          `json:"action,omitempty"`
	Direction    string          `json:"direction,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Log          bool            `json:"log,omitempty"`
	Match        string          `json:"match,omitempty"`
	Meter        libovsdb.OvsSet `json:"meter,omitempty"`
	Name         libovsdb.OvsSet `json:"name,omitempty"`
	Priority     int64           `json:"priority,omitempty"`
	Severity     libovsdb.OvsSet `json:"severity,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Address_Set struct {
	Addresses    libovsdb.OvsSet `json:"addresses,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Name         string          `json:"name,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type BFD struct {
	Detect_mult  libovsdb.OvsSet `json:"detect_mult,omitempty"`
	Dst_ip       string          `json:"dst_ip,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Logical_port string          `json:"logical_port,omitempty"`
	Min_rx       libovsdb.OvsSet `json:"min_rx,omitempty"`
	Min_tx       libovsdb.OvsSet `json:"min_tx,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Status       libovsdb.OvsSet `json:"status,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Connection struct {
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Inactivity_probe libovsdb.OvsSet `json:"inactivity_probe,omitempty"`
	Is_connected     bool            `json:"is_connected,omitempty"`
	Max_backoff      libovsdb.OvsSet `json:"max_backoff,omitempty"`
	Other_config     libovsdb.OvsMap `json:"other_config,omitempty"`
	Status           libovsdb.OvsMap `json:"status,omitempty"`
	Target           string          `json:"target,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type DHCP_Options struct {
	Cidr         string          `json:"cidr,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type DNS struct {
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Records      libovsdb.OvsMap `json:"records,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Forwarding_Group struct {
	Child_port   libovsdb.OvsSet `json:"child_port,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Liveness     bool            `json:"liveness,omitempty"`
	Name         string          `json:"name,omitempty"`
	Vip          string          `json:"vip,omitempty"`
	Vmac         string          `json:"vmac,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Gateway_Chassis struct {
	Chassis_name string          `json:"chassis_name,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Name         string          `json:"name,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Priority     int64           `json:"priority,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type HA_Chassis struct {
	Chassis_name string          `json:"chassis_name,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Priority     int64           `json:"priority,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type HA_Chassis_Group struct {
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Ha_chassis   libovsdb.OvsSet `json:"ha_chassis,omitempty"`
	Name         string          `json:"name,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Load_Balancer struct {
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Health_check     libovsdb.OvsSet `json:"health_check,omitempty"`
	Ip_port_mappings libovsdb.OvsMap `json:"ip_port_mappings,omitempty"`
	Name             string          `json:"name,omitempty"`
	Options          libovsdb.OvsMap `json:"options,omitempty"`
	Protocol         libovsdb.OvsSet `json:"protocol,omitempty"`
	Selection_fields libovsdb.OvsSet `json:"selection_fields,omitempty"`
	Vips             libovsdb.OvsMap `json:"vips,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Load_Balancer_Health_Check struct {
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Vip          string          `json:"vip,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_Router struct {
	Enabled       libovsdb.OvsSet `json:"enabled,omitempty"`
	External_ids  libovsdb.OvsMap `json:"external_ids,omitempty"`
	Load_balancer libovsdb.OvsSet `json:"load_balancer,omitempty"`
	Name          string          `json:"name,omitempty"`
	Nat           libovsdb.OvsSet `json:"nat,omitempty"`
	Options       libovsdb.OvsMap `json:"options,omitempty"`
	Policies      libovsdb.OvsSet `json:"policies,omitempty"`
	Ports         libovsdb.OvsSet `json:"ports,omitempty"`
	Static_routes libovsdb.OvsSet `json:"static_routes,omitempty"`
	Version       libovsdb.UUID   `json:"_version,omitempty"`
	Uuid          libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_Router_Policy struct {
	Action       string          `json:"action,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Match        string          `json:"match,omitempty"`
	Nexthop      libovsdb.OvsSet `json:"nexthop,omitempty"`
	Nexthops     libovsdb.OvsSet `json:"nexthops,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Priority     int64           `json:"priority,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_Router_Port struct {
	Enabled          libovsdb.OvsSet `json:"enabled,omitempty"`
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Gateway_chassis  libovsdb.OvsSet `json:"gateway_chassis,omitempty"`
	Ha_chassis_group libovsdb.OvsSet `json:"ha_chassis_group,omitempty"`
	Ipv6_prefix      libovsdb.OvsSet `json:"ipv6_prefix,omitempty"`
	Ipv6_ra_configs  libovsdb.OvsMap `json:"ipv6_ra_configs,omitempty"`
	Mac              string          `json:"mac,omitempty"`
	Name             string          `json:"name,omitempty"`
	Networks         libovsdb.OvsSet `json:"networks,omitempty"`
	Options          libovsdb.OvsMap `json:"options,omitempty"`
	Peer             libovsdb.OvsSet `json:"peer,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_Router_Static_Route struct {
	Bfd          libovsdb.OvsSet `json:"bfd,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Ip_prefix    string          `json:"ip_prefix,omitempty"`
	Nexthop      string          `json:"nexthop,omitempty"`
	Options      libovsdb.OvsMap `json:"options,omitempty"`
	Output_port  libovsdb.OvsSet `json:"output_port,omitempty"`
	Policy       libovsdb.OvsSet `json:"policy,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_Switch struct {
	Acls              libovsdb.OvsSet `json:"acls,omitempty"`
	Dns_records       libovsdb.OvsSet `json:"dns_records,omitempty"`
	External_ids      libovsdb.OvsMap `json:"external_ids,omitempty"`
	Forwarding_groups libovsdb.OvsSet `json:"forwarding_groups,omitempty"`
	Load_balancer     libovsdb.OvsSet `json:"load_balancer,omitempty"`
	Name              string          `json:"name,omitempty"`
	Other_config      libovsdb.OvsMap `json:"other_config,omitempty"`
	Ports             libovsdb.OvsSet `json:"ports,omitempty"`
	Qos_rules         libovsdb.OvsSet `json:"qos_rules,omitempty"`
	Version           libovsdb.UUID   `json:"_version,omitempty"`
	Uuid              libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Logical_Switch_Port struct {
	Addresses         libovsdb.OvsSet `json:"addresses,omitempty"`
	Dhcpv4_options    libovsdb.OvsSet `json:"dhcpv4_options,omitempty"`
	Dhcpv6_options    libovsdb.OvsSet `json:"dhcpv6_options,omitempty"`
	Dynamic_addresses libovsdb.OvsSet `json:"dynamic_addresses,omitempty"`
	Enabled           libovsdb.OvsSet `json:"enabled,omitempty"`
	External_ids      libovsdb.OvsMap `json:"external_ids,omitempty"`
	Ha_chassis_group  libovsdb.OvsSet `json:"ha_chassis_group,omitempty"`
	Name              string          `json:"name,omitempty"`
	Options           libovsdb.OvsMap `json:"options,omitempty"`
	Parent_name       libovsdb.OvsSet `json:"parent_name,omitempty"`
	Port_security     libovsdb.OvsSet `json:"port_security,omitempty"`
	Tag               libovsdb.OvsSet `json:"tag,omitempty"`
	Tag_request       libovsdb.OvsSet `json:"tag_request,omitempty"`
	Type              string          `json:"type,omitempty"`
	Up                libovsdb.OvsSet `json:"up,omitempty"`
	Version           libovsdb.UUID   `json:"_version,omitempty"`
	Uuid              libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Meter struct {
	Bands        libovsdb.OvsSet `json:"bands,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Fair         libovsdb.OvsSet `json:"fair,omitempty"`
	Name         string          `json:"name,omitempty"`
	Unit         string          `json:"unit,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Meter_Band struct {
	Action       string          `json:"action,omitempty"`
	Burst_size   int64           `json:"burst_size,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Rate         int64           `json:"rate,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type NAT struct {
	Allowed_ext_ips     libovsdb.OvsSet `json:"allowed_ext_ips,omitempty"`
	Exempted_ext_ips    libovsdb.OvsSet `json:"exempted_ext_ips,omitempty"`
	External_ids        libovsdb.OvsMap `json:"external_ids,omitempty"`
	External_ip         string          `json:"external_ip,omitempty"`
	External_mac        libovsdb.OvsSet `json:"external_mac,omitempty"`
	External_port_range string          `json:"external_port_range,omitempty"`
	Logical_ip          string          `json:"logical_ip,omitempty"`
	Logical_port        libovsdb.OvsSet `json:"logical_port,omitempty"`
	Options             libovsdb.OvsMap `json:"options,omitempty"`
	Type                string          `json:"type,omitempty"`
	Version             libovsdb.UUID   `json:"_version,omitempty"`
	Uuid                libovsdb.UUID   `json:"_uuid,omitempty"`
}

type NB_Global struct {
	Connections      libovsdb.OvsSet `json:"connections,omitempty"`
	External_ids     libovsdb.OvsMap `json:"external_ids,omitempty"`
	Hv_cfg           int64           `json:"hv_cfg,omitempty"`
	Hv_cfg_timestamp int64           `json:"hv_cfg_timestamp,omitempty"`
	Ipsec            bool            `json:"ipsec,omitempty"`
	Name             string          `json:"name,omitempty"`
	Nb_cfg           int64           `json:"nb_cfg,omitempty"`
	Nb_cfg_timestamp int64           `json:"nb_cfg_timestamp,omitempty"`
	Options          libovsdb.OvsMap `json:"options,omitempty"`
	Sb_cfg           int64           `json:"sb_cfg,omitempty"`
	Sb_cfg_timestamp int64           `json:"sb_cfg_timestamp,omitempty"`
	Ssl              libovsdb.OvsSet `json:"ssl,omitempty"`
	Version          libovsdb.UUID   `json:"_version,omitempty"`
	Uuid             libovsdb.UUID   `json:"_uuid,omitempty"`
}

type Port_Group struct {
	Acls         libovsdb.OvsSet `json:"acls,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Name         string          `json:"name,omitempty"`
	Ports        libovsdb.OvsSet `json:"ports,omitempty"`
	Version      libovsdb.UUID   `json:"_version,omitempty"`
	Uuid         libovsdb.UUID   `json:"_uuid,omitempty"`
}

type QoS struct {
	Action       libovsdb.OvsMap `json:"action,omitempty"`
	Bandwidth    libovsdb.OvsMap `json:"bandwidth,omitempty"`
	Direction    string          `json:"direction,omitempty"`
	External_ids libovsdb.OvsMap `json:"external_ids,omitempty"`
	Match        string          `json:"match,omitempty"`
	Priority     int64           `json:"priority,omitempty"`
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
