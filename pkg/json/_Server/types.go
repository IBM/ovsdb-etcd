package _Server

import "github.com/roytman/ovsdb-etcd/pkg/json"

type Database struct {
	Schema    string    `json:"schema,omitempty"`
	Cid       json.Uuid `json:"cid,omitempty"`
	Sid       json.Uuid `json:"sid,omitempty"`
	Index     int64     `json:"index,omitempty"`
	Name      string    `json:"name,omitempty"`
	Model     string    `json:"model,omitempty"`
	Connected bool      `json:"connected,omitempty"`
	Leader    bool      `json:"leader,omitempty"`
}
