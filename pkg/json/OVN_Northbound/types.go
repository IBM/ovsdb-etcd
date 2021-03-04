package OVN_Northbound

import "github.com/ibm/ovsdb-etcd/pkg/json"

type ACL struct {
	Action       string            `json:"action,omitempty"`
	Direction    string            `json:"direction,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Log          bool              `json:"log,omitempty"`
	Match        string            `json:"match,omitempty"`
	Meter        string            `json:"meter,omitempty"`
	Name         string            `json:"name,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	Severity     string            `json:"severity,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Address_Set struct {
	Addresses    []string          `json:"addresses,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Name         string            `json:"name,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Connection struct {
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Inactivity_probe int64             `json:"inactivity_probe,omitempty"`
	Is_connected     bool              `json:"is_connected,omitempty"`
	Max_backoff      int64             `json:"max_backoff,omitempty"`
	Other_config     map[string]string `json:"other_config,omitempty"`
	Status           map[string]string `json:"status,omitempty"`
	Target           string            `json:"target,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type DHCP_Options struct {
	Cidr         string            `json:"cidr,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type DNS struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Records      map[string]string `json:"records,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Forwarding_Group struct {
	Child_port   []string          `json:"child_port,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Liveness     bool              `json:"liveness,omitempty"`
	Name         string            `json:"name,omitempty"`
	Vip          string            `json:"vip,omitempty"`
	Vmac         string            `json:"vmac,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Gateway_Chassis struct {
	Chassis_name string            `json:"chassis_name,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Name         string            `json:"name,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type HA_Chassis struct {
	Chassis_name string            `json:"chassis_name,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type HA_Chassis_Group struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Ha_chassis   []json.Uuid       `json:"ha_chassis,omitempty"`
	Name         string            `json:"name,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Load_Balancer struct {
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Health_check     []json.Uuid       `json:"health_check,omitempty"`
	Ip_port_mappings map[string]string `json:"ip_port_mappings,omitempty"`
	Name             string            `json:"name,omitempty"`
	Options          map[string]string `json:"options,omitempty"`
	Protocol         string            `json:"protocol,omitempty"`
	Selection_fields []string          `json:"selection_fields,omitempty"`
	Vips             map[string]string `json:"vips,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type Load_Balancer_Health_Check struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Vip          string            `json:"vip,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Logical_Router struct {
	Enabled       bool              `json:"enabled,omitempty"`
	External_ids  map[string]string `json:"external_ids,omitempty"`
	Load_balancer []json.Uuid       `json:"load_balancer,omitempty"`
	Name          string            `json:"name,omitempty"`
	Nat           []json.Uuid       `json:"nat,omitempty"`
	Options       map[string]string `json:"options,omitempty"`
	Policies      []json.Uuid       `json:"policies,omitempty"`
	Ports         []json.Uuid       `json:"ports,omitempty"`
	Static_routes []json.Uuid       `json:"static_routes,omitempty"`
	Version       json.Uuid         `json:"_version,omitempty"`
	Uuid          json.Uuid         `json:"uuid,omitempty"`
}

type Logical_Router_Policy struct {
	Action       string            `json:"action,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Match        string            `json:"match,omitempty"`
	Nexthop      string            `json:"nexthop,omitempty"`
	Nexthops     []string          `json:"nexthops,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Logical_Router_Port struct {
	Enabled          bool              `json:"enabled,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Gateway_chassis  []json.Uuid       `json:"gateway_chassis,omitempty"`
	Ha_chassis_group json.Uuid         `json:"ha_chassis_group,omitempty"`
	Ipv6_prefix      []string          `json:"ipv6_prefix,omitempty"`
	Ipv6_ra_configs  map[string]string `json:"ipv6_ra_configs,omitempty"`
	Mac              string            `json:"mac,omitempty"`
	Name             string            `json:"name,omitempty"`
	Networks         []string          `json:"networks,omitempty"`
	Options          map[string]string `json:"options,omitempty"`
	Peer             string            `json:"peer,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type Logical_Router_Static_Route struct {
	External_ids map[string]string `json:"external_ids,omitempty"`
	Ip_prefix    string            `json:"ip_prefix,omitempty"`
	Nexthop      string            `json:"nexthop,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	Output_port  string            `json:"output_port,omitempty"`
	Policy       string            `json:"policy,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Logical_Switch struct {
	Acls              []json.Uuid       `json:"acls,omitempty"`
	Dns_records       []json.Uuid       `json:"dns_records,omitempty"`
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Forwarding_groups []json.Uuid       `json:"forwarding_groups,omitempty"`
	Load_balancer     []json.Uuid       `json:"load_balancer,omitempty"`
	Name              string            `json:"name,omitempty"`
	Other_config      map[string]string `json:"other_config,omitempty"`
	Ports             []json.Uuid       `json:"ports,omitempty"`
	Qos_rules         []json.Uuid       `json:"qos_rules,omitempty"`
	Version           json.Uuid         `json:"_version,omitempty"`
	Uuid              json.Uuid         `json:"uuid,omitempty"`
}

type Logical_Switch_Port struct {
	Addresses         []string          `json:"addresses,omitempty"`
	Dhcpv4_options    json.Uuid         `json:"dhcpv4_options,omitempty"`
	Dhcpv6_options    json.Uuid         `json:"dhcpv6_options,omitempty"`
	Dynamic_addresses string            `json:"dynamic_addresses,omitempty"`
	Enabled           bool              `json:"enabled,omitempty"`
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Ha_chassis_group  json.Uuid         `json:"ha_chassis_group,omitempty"`
	Name              string            `json:"name,omitempty"`
	Options           map[string]string `json:"options,omitempty"`
	Parent_name       string            `json:"parent_name,omitempty"`
	Port_security     []string          `json:"port_security,omitempty"`
	Tag               int64             `json:"tag,omitempty"`
	Tag_request       int64             `json:"tag_request,omitempty"`
	Type              string            `json:"type,omitempty"`
	Up                bool              `json:"up,omitempty"`
	Version           json.Uuid         `json:"_version,omitempty"`
	Uuid              json.Uuid         `json:"uuid,omitempty"`
}

type Meter struct {
	Bands        []json.Uuid       `json:"bands,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Fair         bool              `json:"fair,omitempty"`
	Name         string            `json:"name,omitempty"`
	Unit         string            `json:"unit,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type Meter_Band struct {
	Action       string            `json:"action,omitempty"`
	Burst_size   int64             `json:"burst_size,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Rate         int64             `json:"rate,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type NAT struct {
	Allowed_ext_ips     json.Uuid         `json:"allowed_ext_ips,omitempty"`
	Exempted_ext_ips    json.Uuid         `json:"exempted_ext_ips,omitempty"`
	External_ids        map[string]string `json:"external_ids,omitempty"`
	External_ip         string            `json:"external_ip,omitempty"`
	External_mac        string            `json:"external_mac,omitempty"`
	External_port_range string            `json:"external_port_range,omitempty"`
	Logical_ip          string            `json:"logical_ip,omitempty"`
	Logical_port        string            `json:"logical_port,omitempty"`
	Options             map[string]string `json:"options,omitempty"`
	Type                string            `json:"type,omitempty"`
	Version             json.Uuid         `json:"_version,omitempty"`
	Uuid                json.Uuid         `json:"uuid,omitempty"`
}

type NB_Global struct {
	Connections      []json.Uuid       `json:"connections,omitempty"`
	External_ids     map[string]string `json:"external_ids,omitempty"`
	Hv_cfg           int64             `json:"hv_cfg,omitempty"`
	Hv_cfg_timestamp int64             `json:"hv_cfg_timestamp,omitempty"`
	Ipsec            bool              `json:"ipsec,omitempty"`
	Name             string            `json:"name,omitempty"`
	Nb_cfg           int64             `json:"nb_cfg,omitempty"`
	Nb_cfg_timestamp int64             `json:"nb_cfg_timestamp,omitempty"`
	Options          map[string]string `json:"options,omitempty"`
	Sb_cfg           int64             `json:"sb_cfg,omitempty"`
	Sb_cfg_timestamp int64             `json:"sb_cfg_timestamp,omitempty"`
	Ssl              json.Uuid         `json:"ssl,omitempty"`
	Version          json.Uuid         `json:"_version,omitempty"`
	Uuid             json.Uuid         `json:"uuid,omitempty"`
}

type Port_Group struct {
	Acls         []json.Uuid       `json:"acls,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Name         string            `json:"name,omitempty"`
	Ports        []json.Uuid       `json:"ports,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type QoS struct {
	Action       map[string]int64  `json:"action,omitempty"`
	Bandwidth    map[string]int64  `json:"bandwidth,omitempty"`
	Direction    string            `json:"direction,omitempty"`
	External_ids map[string]string `json:"external_ids,omitempty"`
	Match        string            `json:"match,omitempty"`
	Priority     int64             `json:"priority,omitempty"`
	Version      json.Uuid         `json:"_version,omitempty"`
	Uuid         json.Uuid         `json:"uuid,omitempty"`
}

type SSL struct {
	Bootstrap_ca_cert bool              `json:"bootstrap_ca_cert,omitempty"`
	Ca_cert           string            `json:"ca_cert,omitempty"`
	Certificate       string            `json:"certificate,omitempty"`
	External_ids      map[string]string `json:"external_ids,omitempty"`
	Private_key       string            `json:"private_key,omitempty"`
	Ssl_ciphers       string            `json:"ssl_ciphers,omitempty"`
	Ssl_protocols     string            `json:"ssl_protocols,omitempty"`
	Version           json.Uuid         `json:"_version,omitempty"`
	Uuid              json.Uuid         `json:"uuid,omitempty"`
}
