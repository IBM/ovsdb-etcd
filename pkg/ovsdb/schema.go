package ovsdb

type NB_Global struct {
	Uuid             string            `json:"_uuid"`
	Name             string            `json:"name"`
	Nb_cfg           int               `json:"nb_cfg"`
	Nb_cfg_timestamp int               `json:"nb_cfg_timestamp"`
	Sb_cfg           int               `json:"sb_cfg"`
	Sb_cfg_timestamp int               `json:"sb_cfg_timestamp"`
	Hv_cfg           int               `json:"hv_cfg"`
	Hv_cfg_timestamp int               `json:"hv_cfg_timestamp"`
	External_ids     map[string]string `json:"external_ids"`
	Connections      []string          `json:"connections"`
	Ssl              []string          `json:"ssl"`
	Options          map[string]string `json:"options"`
	Ipsec            bool              `json:"ipsec"`
}

type Logical_Switch struct {
	Uuid              string            `json:"_uuid"`
	Name              string            `json:"name"`
	Ports             []string          `json:"ports"`
	Acls              []string          `json:"acls"`
	Qos_rule          []string          `json:"qos_rule"`
	Load_balancer     []string          `json:"load_balancer"`
	Dns_records       []string          `json:"dns_records"`
	Other_config      map[string]string `json:"other_config"`
	External_ids      map[string]string `json:"external_ids"`
	Forwarding_groups []string          `json:"forwarding_groups"`
}

type Logical_Switch_Port struct {
	Uuid              string            `json:"_uuid"`
	Name              string            `json:"name"`
	Type              string            `json:"type"`
	Options           map[string]string `json:"options"`
	Parent_name       []string          `json:"parent_name"`
	Tag_request       []int             `json:"tag_request"`
	Tag               []int             `json:"tag"`
	Addresses         []string          `json:"addresses"`
	Dynamic_addresses []string          `json:"dynamic_addresses"`
	Port_security     []string          `json:"port_security"`
	Up                []bool            `json:"up"`
	Enabled           []bool            `json:"enabled"`
	Dhcpv4_options    []string          `json:"dhcpv4_options"`
	Dhcpv6_options    []string          `json:"dhcpv6_options"`
	Ha_chassis_group  []string          `json:"ha_chassis_group"`
	External_ids      map[string]string `json:"external_ids"`
}

type Forwarding_Group struct {
	Uuid         string            `json:"_uuid"`
	Name         string            `json:"name"`
	Vip          string            `json:"vip"`
	Vmac         string            `json:"vmac"`
	Liveness     bool              `json:"liveness"`
	External_ids map[string]string `json:"external_ids"`
	Child_port   []string          `json:"child_port"`
}

type Address_Set struct {
	Uuid         string            `json:"_uuid"`
	Name         string            `json:"name"`
	Adresses     []string          `json:"adresses"`
	External_ids map[string]string `json:"external_ids"`
}

type Port_Group struct {
	Uuid         string            `json:"_uuid"`
	Name         string            `json:"name"`
	Ports        []string          `json:"ports"`
	Acls         []string          `json:"acls"`
	External_ids map[string]string `json:"external_ids"`
}

type Load_Balancer struct {
	Uuid             string            `json:"_uuid"`
	Name             string            `json:"name"`
	Vips             map[string]string `json:"vips"`
	Protocol         []string          `json:"protocol"`
	Health_check     []string          `json:"health_check"`
	Ip_port_mappings map[string]string `json:"ip_port_mappings"`
	Selection_fields []string          `json:"selection_fields"`
	Options          map[string]string `json:"options"`
	External_ids     map[string]string `json:"external_ids"`
}

type Load_Balancer_Health_Check struct {
	Uuid         string            `json:"_uuid"`
	Vip          string            `json:"vip"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type ACL struct {
	Uuid         string            `json:"_uuid"`
	Name         []string          `json:"name"`
	Priority     []int             `json:"priority"`
	Direction    []string          `json:"direction"`
	Match        string            `json:"match"`
	Action       string            `json:"action"`
	Log          bool              `json:"log"`
	Severity     []string          `json:"severity"`
	Meter        []string          `json:"meter"`
	External_ids map[string]string `json:"external_ids"`
}

type QoS struct {
	Uuid         string            `json:"_uuid"`
	Priority     int               `json:"priority"`
	Direction    []string          `json:"direction"`
	Match        string            `json:"match"`
	Action       map[string]int    `json:"action"`
	Bandwidth    map[string]int    `json:"bandwidth"`
	External_ids map[string]string `json:"external_ids"`
}

type Meter struct {
	Uuid         string            `json:"_uuid"`
	Name         string            `json:"name"`
	Unit         string            `json:"unit"`
	Bands        []string          `json:"bands"`
	Fair         []bool            `json:"fair"`
	External_ids map[string]string `json:"external_ids"`
}

type Meter_Band struct {
	Uuid         string            `json:"_uuid"`
	Action       []string          `json:"action"`
	Rate         int               `json:"rate"`
	Burst_size   int               `json:"burst_size"`
	External_ids map[string]string `json:"external_ids"`
}

type Logical_Router struct {
	Uuid          string            `json:"_uuid"`
	Name          string            `json:"name"`
	Ports         []string          `json:"ports"`
	Static_routes []string          `json:"static_routes"`
	Policies      []string          `json:"policies"`
	Enabled       []bool            `json:"enabled"`
	Nat           []string          `json:"nat"`
	Load_balancer []string          `json:"load_balancer"`
	Options       map[string]string `json:"options"`
	External_ids  map[string]string `json:"external_ids"`
}

type Logical_Router_Port struct {
	Uuid             string            `json:"_uuid"`
	Name             string            `json:"name"`
	Gateway_chassis  []string          `json:"gateway_chassis"`
	Ha_chassis_group []string          `json:"ha_chassis_group"`
	Networks         []string          `json:"networks"`
	Mac              string            `json:"Mac"`
	Peer             []string          `json:"peer"`
	Enabled          []bool            `json:"enabled"`
	Ipv6_ra_configs  map[string]string `json:"ipv6_ra_configs"`
	Ipv6_prefix      []string          `json:"ipv6_prefix"`
	Options          map[string]string `json:"options"`
	External_ids     map[string]string `json:"external_ids"`
}

type Logical_Router_Static_Route struct {
	Uuid         string            `json:"_uuid"`
	Ip_prefix    string            `json:"ip_prefix"`
	Policy       string            `json:"policy"`
	Nexthop      string            `json:"nexthop"`
	Output_port  string            `json:"output_port"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type Logical_Router_Policy struct {
	Uuid         string            `json:"_uuid"`
	Priority     int               `json:"priority"`
	Match        string            `json:"match"`
	Action       string            `json:"action"`
	Nexthop      string            `json:"nexthop"`
	Nexthops     []string          `json:"nexthops"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type NAT struct {
	Uuid                string            `json:"_uuid"`
	External_ip         string            `json:"external_ip"`
	External_mac        string            `json:"external_mac"`
	External_port_range string            `json:"external_port_range"`
	Logical_ip          string            `json:"logical_ip"`
	Logical_port        string            `json:"logical_port"`
	Type                string            `json:"type"`
	Allowed_ext_ips     []string          `json:"allowed_ext_ips"`
	Exempted_ext_ips    []string          `json:"exempted_ext_ips"`
	Options             map[string]string `json:"options"`
	External_ids        map[string]string `json:"external_ids"`
}

type DHCP_Options struct {
	Uuid         string            `json:"_uuid"`
	Cidr         string            `json:"cidr"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type Connection struct {
	Uuid             string            `json:"_uuid"`
	Target           string            `json:"target"`
	Max_backoff      []int             `json:"max_backoff"`
	Inactivity_probe int               `json:"inactivity_probe"`
	Other_config     []string          `json:"other_config"`
	Is_connected     bool              `json:"is_connected"`
	Status           map[string]string `json:"status"`
	External_ids     map[string]string `json:"external_ids"`
}

type DNS struct {
	Uuid         string            `json:"_uuid"`
	Records      map[string]string `json:"records"`
	External_ids map[string]string `json:"external_ids"`
}

type SSL struct {
	Uuid              string            `json:"_uuid"`
	Private_key       string            `json:"private_key"`
	Certificate       string            `json:"certificate"`
	Ca_cert           string            `json:"ca_cert"`
	Bootstrap_ca_cert bool              `json:"bootstrap_ca_cert"`
	Ssl_protocols     string            `json:"ssl_protocols"`
	Ssl_ciphers       string            `json:"ssl_ciphers"`
	External_ids      map[string]string `json:"external_ids"`
}

type Gateway_Chassis struct {
	Uuid         string            `json:"_uuid"`
	Name         string            `json:"name"`
	Chassis_name string            `json:"chassis_name"`
	Priority     int               `json:"priority"`
	Options      map[string]string `json:"options"`
	External_ids map[string]string `json:"external_ids"`
}

type HA_Chassis struct {
	Uuid         string            `json:"_uuid"`
	Chassis_name string            `json:"chassis_name"`
	Priority     int               `json:"priority"`
	External_ids map[string]string `json:"external_ids"`
}

type HA_Chassis_Group struct {
	Uuid         string            `json:"_uuid"`
	Name         string            `json:"name"`
	Ha_chassis   []string          `json:"ha_chassis"`
	External_ids map[string]string `json:"external_ids"`
}
