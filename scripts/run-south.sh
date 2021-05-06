#!/bin/bash

until curl http://127.0.0.1:2479; do sleep 2;done ;server -tcp-address=6642 -unix-address=/etc/ovn/ovnsb_db.db -schema-basedir=./schemas -database-prefix=ovsdb -service-name=sb -schema-file=ovn-sb.ovsschema -schema-name=OVN_Southbound -load-server-data=TRUE -etcd-members=localhost:2479
