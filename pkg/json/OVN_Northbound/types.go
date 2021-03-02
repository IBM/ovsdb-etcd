package OVN_Northbound

import "github.com/roytman/ovsdb-etcd/pkg/json"

type DNS struct {
	Records      map[string]string `json:"records,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type Logical_Switch struct {
	Load_balancer     []json.Uuid       `json:"load_balancer,omitempty"`
	Dns_records       []json.Uuid       `json:"dns_records,omitempty"`
	Other_config      map[string]string `json:"other_config,omitempty"`
	Acls              []json.Uuid       `json:"acls,omitempty"`
	Ports             []json.Uuid       `json:"ports,omitempty"`
	Qos_rules         []json.Uuid       `json:"qos_rules,omitempty"`
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Forwarding_groups []json.Uuid       `json:"forwarding_groups,omitempty"`
	Name              string            `json:"name,omitempty"`
}

type Load_Balancer struct {
	Health_check     []json.Uuid       `json:"health_check,omitempty"`
	Ip_port_mappings map[string]string `json:"ip_port_mappings,omitempty"`
	Selection_fields []string          `json:"selection_fields,omitempty"`
	Options          map[string]string `json:"options,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Name             string            `json:"name,omitempty"`
	Vips             map[string]string `json:"vips,omitempty"`
	Protocol         string            `json:"protocol,omitempty"`
}

type Meter struct {
	Bands        []json.Uuid       `json:"bands,omitempty"`
	Fair         bool              `json:"fair,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Name         string            `json:"name,omitempty"`
	Unit         string            `json:"unit,omitempty"`
}

type Connection struct {
	Max_backoff      int64             `json:"max_backoff,omitempty"`
	Inactivity_probe int64             `json:"inactivity_probe,omitempty"`
	Other_config     map[string]string `json:"other_config,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Is_connected     bool              `json:"is_connected,omitempty"`
	Status           map[string]string `json:"status,omitempty"`
	Target           string            `json:"target,omitempty"`
}

type NB_Global struct {
	Hv_cfg_timestamp int64             `json:"hv_cfg_timestamp,omitempty"`
	Ssl              json.Uuid         `json:"ssl,omitempty"`
	Ipsec            bool              `json:"ipsec,omitempty"`
	Nb_cfg           int64             `json:"nb_cfg,omitempty"`
	Nb_cfg_timestamp int64             `json:"nb_cfg_timestamp,omitempty"`
	Sb_cfg           int64             `json:"sb_cfg,omitempty"`
	Hv_cfg           int64             `json:"hv_cfg,omitempty"`
	Options          map[string]string `json:"options,omitempty"`
	Name             string            `json:"name,omitempty"`
	Sb_cfg_timestamp int64             `json:"sb_cfg_timestamp,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Connections      []json.Uuid       `json:"connections,omitempty"`
}

type Forwarding_Group struct {
	Name         string            `json:"name,omitempty"`
	Vip          string            `json:"vip,omitempty"`
	Vmac         string            `json:"vmac,omitempty"`
	Liveness     bool              `json:"liveness,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Child_port   []string          `json:"child_port,omitempty"`
}

type Port_Group struct {
	Name         string            `json:"name,omitempty"`
	Ports        []json.Uuid       `json:"ports,omitempty"`
	Acls         []json.Uuid       `json:"acls,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type SSL struct {
	Ssl_ciphers       string            `json:"ssl_ciphers,omitempty"`
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Private_key       string            `json:"private_key,omitempty"`
	Certificate       string            `json:"certificate,omitempty"`
	Ca_cert           string            `json:"ca_cert,omitempty"`
	Bootstrap_ca_cert bool              `json:"bootstrap_ca_cert,omitempty"`
	Ssl_protocols     string            `json:"ssl_protocols,omitempty"`
}

type Logical_Switch_Port struct {
	Up                bool              `json:"up,omitempty"`
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Dynamic_addresses string            `json:"dynamic_addresses,omitempty"`
	Enabled           bool              `json:"enabled,omitempty"`
	Options           map[string]string `json:"options,omitempty"`
	Parent_name       string            `json:"parent_name,omitempty"`
	Tag_request       int64             `json:"tag_request,omitempty"`
	Tag               int64             `json:"tag,omitempty"`
	Port_security     []string          `json:"port_security,omitempty"`
	Dhcpv4_options    json.Uuid         `json:"dhcpv4_options,omitempty"`
	Dhcpv6_options    json.Uuid         `json:"dhcpv6_options,omitempty"`
	Name              string            `json:"name,omitempty"`
	Type              string            `json:"type,omitempty"`
	Addresses         []string          `json:"addresses,omitempty"`
	Ha_chassis_group  json.Uuid         `json:"ha_chassis_group,omitempty"`
}

type Load_Balancer_Health_Check struct {
	Vip          string            `json:"vip,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type DHCP_Options struct {
	Cidr         string            `json:"cidr,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type HA_Chassis struct {
	Chassis_name string            `json:"chassis_name,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type Meter_Band struct {
	Action       string            `json:"action,omitempty"`
	Rate         int64             `json:"rate,omitempty"`
	Burst_size   int64             `json:"burst_size,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type Logical_Router_Policy struct {
	Action       string            `json:"action,omitempty"`
	Nexthop      string            `json:"nexthop,omitempty"`
	Nexthops     []string          `json:"nexthops,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	Match        string            `json:"match,omitempty"`
}

type Gateway_Chassis struct {
	Name         string            `json:"name,omitempty"`
	Chassis_name string            `json:"chassis_name,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
}

type Address_Set struct {
	Name         string            `json:"name,omitempty"`
	Addresses    []string          `json:"addresses,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
}

type QoS struct {
	Direction    string            `json:"direction,omitempty"`
	Match        string            `json:"match,omitempty"`
	Action       map[string]int64  `json:"action,omitempty"`
	Bandwidth    map[string]int64  `json:"bandwidth,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
}

type Logical_Router_Static_Route struct {
	Nexthop      string            `json:"nexthop,omitempty"`
	Output_port  string            `json:"output_port,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Ip_prefix    string            `json:"ip_prefix,omitempty"`
	Policy       string            `json:"policy,omitempty"`
}

type ACL struct {
	Match        string            `json:"match,omitempty"`
	Meter        string            `json:"meter,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Name         string            `json:"name,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	Log          bool              `json:"log,omitempty"`
	Severity     string            `json:"severity,omitempty"`
	Direction    string            `json:"direction,omitempty"`
	Action       string            `json:"action,omitempty"`
}

type Logical_Router struct {
	Ports         []json.Uuid       `json:"ports,omitempty"`
	Enabled       bool              `json:"enabled,omitempty"`
	Nat           []json.Uuid       `json:"nat,omitempty"`
	Options       map[string]string `json:"options,omitempty"`
	External_ids  map[string]string `json:"external_ids,omitempty"`
	Name          string            `json:"name,omitempty"`
	Static_routes []json.Uuid       `json:"static_routes,omitempty"`
	Policies      []json.Uuid       `json:"policies,omitempty"`
	Load_balancer []json.Uuid       `json:"load_balancer,omitempty"`
}

type NAT struct {
	Exempted_ext_ips    json.Uuid         `json:"exempted_ext_ips,omitempty"`
	External_mac        string            `json:"external_mac,omitempty"`
	Logical_port        string            `json:"logical_port,omitempty"`
	Logical_ip          string            `json:"logical_ip,omitempty"`
	Type                string            `json:"type,omitempty"`
	Allowed_ext_ips     json.Uuid         `json:"allowed_ext_ips,omitempty"`
	Options             map[string]string `json:"options,omitempty"`
	External_ids        map[string]string `json:"external_ids,omitempty"`
	External_ip         string            `json:"external_ip,omitempty"`
	External_port_range string            `json:"external_port_range,omitempty"`
}

type HA_Chassis_Group struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Name         string            `json:"name,omitempty"`
	Ha_chassis   []json.Uuid       `json:"ha_chassis,omitempty"`
}

type Logical_Router_Port struct {
	Options          map[string]string `json:"options,omitempty"`
	Peer             string            `json:"peer,omitempty"`
	Enabled          bool              `json:"enabled,omitempty"`
	Ipv6_prefix      []string          `json:"ipv6_prefix,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Gateway_chassis  []json.Uuid       `json:"gateway_chassis,omitempty"`
	Ha_chassis_group json.Uuid         `json:"ha_chassis_group,omitempty"`
	Mac              string            `json:"mac,omitempty"`
	Ipv6_ra_configs  map[string]string `json:"ipv6_ra_configs,omitempty"`
	Name             string            `json:"name,omitempty"`
	Networks         []string          `json:"networks,omitempty"`
}
