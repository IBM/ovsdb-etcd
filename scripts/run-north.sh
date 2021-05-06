#!/bin/bash

until curl http://127.0.0.1:2479; do sleep 2;done ;server -tcp-address=6641 -unix-address=/etc/ovn/ovnnb_db.db -schema-basedir=./schemas -database-prefix=ovsdb -service-name=nb -schema-file=ovn-nb.ovsschema -schema-name=OVN_NorthBound -load-server-data=TRUE -etcd-members=localhost:2479
