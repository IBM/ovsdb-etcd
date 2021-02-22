package _Server

import "github.com/roytman/ovsdb-etcd/pkg/json"

type Database struct {
	Name      string    `json:"name"`
	Model     string    `json:"model"`
	Connected bool      `json:"connected"`
	Leader    bool      `json:"leader"`
	Schema    string    `json:"schema"`
	Cid       json.Uuid `json:"cid"`
	Sid       json.Uuid `json:"sid"`
	Index     int64     `json:"index"`
}
