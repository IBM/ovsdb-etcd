#!/bin/bash

source /usr/local/sbin/demo-magic.sh

reset

TCP_ADDRESS=tcp:127.0.0.1:12345
UNIX_ADDRESS=unix:/tmp/ovsdb-etcd.sock

pe "ovsdb-client -v dump $TCP_ADDRESS _Server"

pe "ovsdb-client -v list-dbs $TCP_ADDRESS"

pe "ovsdb-client -v list-tables $TCP_ADDRESS OVN_Northbound"

pe "ovn-nbctl -v --db=$UNIX_ADDRESS show"
