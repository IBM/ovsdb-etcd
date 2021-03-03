package _Server

import "github.com/roytman/ovsdb-etcd/pkg/json"

type Database struct {
	Model     string    `json:"model,omitempty"`
	Connected bool      `json:"connected,omitempty"`
	Leader    bool      `json:"leader,omitempty"`
	Schema    string    `json:"schema,omitempty"`
	Cid       json.Uuid `json:"cid,omitempty"`
	Sid       json.Uuid `json:"sid,omitempty"`
	Index     int64     `json:"index,omitempty"`
	Name      string    `json:"name,omitempty"`
	Version   json.Uuid `json:"_version,omitempty"`
	Uuid      json.Uuid `json:"uuid,omitempty"`
}
