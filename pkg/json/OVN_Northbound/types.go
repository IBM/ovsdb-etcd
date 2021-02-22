package OVN_Northbound

import "github.com/roytman/ovsdb-etcd/pkg/json"

type NB_Global struct {
	Sb_cfg           int64             `json:"sb_cfg"`
	Sb_cfg_timestamp int64             `json:"sb_cfg_timestamp"`
	Hv_cfg_timestamp int64             `json:"hv_cfg_timestamp"`
	External_ids     map[string]string `json:"external_ids"`
	Options          map[string]string `json:"options"`
	Ssl              json.Uuid         `json:"ssl"`
	Ipsec            bool              `json:"ipsec"`
	Name             string            `json:"name"`
	Nb_cfg           int64             `json:"nb_cfg"`
	Nb_cfg_timestamp int64             `json:"nb_cfg_timestamp"`
	Hv_cfg           int64             `json:"hv_cfg"`
	Connections      []json.Uuid       `json:"connections"`
}

type Logical_Router struct {
	Load_balancer []json.Uuid       `json:"load_balancer"`
	Options       map[string]string `json:"options"`
	External_ids  map[string]string `json:"external_ids"`
	Name          string            `json:"name"`
	Ports         []json.Uuid       `json:"ports"`
	Static_routes []json.Uuid       `json:"static_routes"`
	Policies      []json.Uuid       `json:"policies"`
	Nat           []json.Uuid       `json:"nat"`
	Enabled       bool              `json:"enabled"`
}

type Meter_Band struct {
	Action       string            `json:"action"`
	Rate         int64             `json:"rate"`
	Burst_size   int64             `json:"burst_size"`
	External_ids map[string]string `json:"external_ids"`
}

type SSL struct {
	Private_key       string            `json:"private_key"`
	Certificate       string            `json:"certificate"`
	Ca_cert           string            `json:"ca_cert"`
	Bootstrap_ca_cert bool              `json:"bootstrap_ca_cert"`
	Ssl_protocols     string            `json:"ssl_protocols"`
	Ssl_ciphers       string            `json:"ssl_ciphers"`
	External_ids      map[string]string `json:"external_ids"`
}

type HA_Chassis struct {
	Chassis_name string            `json:"chassis_name"`
	Priority     int64             `json:"priority"`
	External_ids map[string]string `json:"external_ids"`
}

type NAT struct {
	External_mac        string            `json:"external_mac"`
	Exempted_ext_ips    json.Uuid         `json:"exempted_ext_ips"`
	Options             map[string]string `json:"options"`
	External_ip         string            `json:"external_ip"`
	External_port_range string            `json:"external_port_range"`
	Logical_ip          string            `json:"logical_ip"`
	Logical_port        string            `json:"logical_port"`
	Type                string            `json:"type"`
	Allowed_ext_ips     json.Uuid         `json:"allowed_ext_ips"`
	External_ids        map[string]string `json:"external_ids"`
}

type Logical_Router_Static_Route struct {
	Nexthop      string            `json:"nexthop"`
	Output_port  string            `json:"output_port"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
	Ip_prefix    string            `json:"ip_prefix"`
	Policy       string            `json:"policy"`
}

type Meter struct {
	Name         string            `json:"name"`
	Unit         string            `json:"unit"`
	Bands        []json.Uuid       `json:"bands"`
	Fair         bool              `json:"fair"`
	External_ids map[string]string `json:"external_ids"`
}

type Gateway_Chassis struct {
	External_ids map[string]string `json:"external_ids"`
	Options      map[string]string `json:"options"`
	Name         string            `json:"name"`
	Chassis_name string            `json:"chassis_name"`
	Priority     int64             `json:"priority"`
}

type QoS struct {
	Action       map[string]int64  `json:"action"`
	Bandwidth    map[string]int64  `json:"bandwidth"`
	External_ids map[string]string `json:"external_ids"`
	Priority     int64             `json:"priority"`
	Direction    string            `json:"direction"`
	Match        string            `json:"match"`
}

type Load_Balancer_Health_Check struct {
	Vip          string            `json:"vip"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type ACL struct {
	Log          bool              `json:"log"`
	Severity     string            `json:"severity"`
	Action       string            `json:"action"`
	Priority     int64             `json:"priority"`
	Direction    string            `json:"direction"`
	Match        string            `json:"match"`
	Meter        string            `json:"meter"`
	External_ids map[string]string `json:"external_ids"`
	Name         string            `json:"name"`
}

type Port_Group struct {
	Name         string            `json:"name"`
	Ports        []json.Uuid       `json:"ports"`
	Acls         []json.Uuid       `json:"acls"`
	External_ids map[string]string `json:"external_ids"`
}

type Logical_Switch struct {
	Other_config      map[string]string `json:"other_config"`
	Name              string            `json:"name"`
	Acls              []json.Uuid       `json:"acls"`
	Qos_rules         []json.Uuid       `json:"qos_rules"`
	Load_balancer     []json.Uuid       `json:"load_balancer"`
	Dns_records       []json.Uuid       `json:"dns_records"`
	Ports             []json.Uuid       `json:"ports"`
	External_ids      map[string]string `json:"external_ids"`
	Forwarding_groups []json.Uuid       `json:"forwarding_groups"`
}

type Logical_Switch_Port struct {
	Parent_name       string            `json:"parent_name"`
	Tag_request       int64             `json:"tag_request"`
	Addresses         []string          `json:"addresses"`
	Dynamic_addresses string            `json:"dynamic_addresses"`
	Enabled           bool              `json:"enabled"`
	Dhcpv4_options    json.Uuid         `json:"dhcpv4_options"`
	Ha_chassis_group  json.Uuid         `json:"ha_chassis_group"`
	Name              string            `json:"name"`
	Tag               int64             `json:"tag"`
	External_ids      map[string]string `json:"external_ids"`
	Type              string            `json:"type"`
	Options           map[string]string `json:"options"`
	Port_security     []string          `json:"port_security"`
	Up                bool              `json:"up"`
	Dhcpv6_options    json.Uuid         `json:"dhcpv6_options"`
}

type DNS struct {
	Records      map[string]string `json:"records"`
	External_ids map[string]string `json:"external_ids"`
}

type HA_Chassis_Group struct {
	Name         string            `json:"name"`
	Ha_chassis   []json.Uuid       `json:"ha_chassis"`
	External_ids map[string]string `json:"external_ids"`
}

type Load_Balancer struct {
	Name             string            `json:"name"`
	Vips             map[string]string `json:"vips"`
	Protocol         string            `json:"protocol"`
	Health_check     []json.Uuid       `json:"health_check"`
	Ip_port_mappings map[string]string `json:"ip_port_mappings"`
	Selection_fields []string          `json:"selection_fields"`
	Options          map[string]string `json:"options"`
	External_ids     map[string]string `json:"external_ids"`
}

type Logical_Router_Port struct {
	Mac              string            `json:"mac"`
	Peer             string            `json:"peer"`
	Enabled          bool              `json:"enabled"`
	Ipv6_ra_configs  map[string]string `json:"ipv6_ra_configs"`
	Ipv6_prefix      []string          `json:"ipv6_prefix"`
	External_ids     map[string]string `json:"external_ids"`
	Ha_chassis_group json.Uuid         `json:"ha_chassis_group"`
	Gateway_chassis  []json.Uuid       `json:"gateway_chassis"`
	Options          map[string]string `json:"options"`
	Networks         []string          `json:"networks"`
	Name             string            `json:"name"`
}

type Forwarding_Group struct {
	Vmac         string            `json:"vmac"`
	Liveness     bool              `json:"liveness"`
	External_ids map[string]string `json:"external_ids"`
	Child_port   []string          `json:"child_port"`
	Name         string            `json:"name"`
	Vip          string            `json:"vip"`
}

type Logical_Router_Policy struct {
	Priority     int64             `json:"priority"`
	Match        string            `json:"match"`
	Action       string            `json:"action"`
	Nexthop      string            `json:"nexthop"`
	Nexthops     []string          `json:"nexthops"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type DHCP_Options struct {
	Cidr         string            `json:"cidr"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type Connection struct {
	External_ids     map[string]string `json:"external_ids"`
	Is_connected     bool              `json:"is_connected"`
	Status           map[string]string `json:"status"`
	Target           string            `json:"target"`
	Max_backoff      int64             `json:"max_backoff"`
	Inactivity_probe int64             `json:"inactivity_probe"`
	Other_config     map[string]string `json:"other_config"`
}

type Address_Set struct {
	Addresses    []string          `json:"addresses"`
	External_ids map[string]string `json:"external_ids"`
	Name         string            `json:"name"`
}
