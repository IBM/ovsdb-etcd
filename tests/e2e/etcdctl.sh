#!/bin/bash

source /usr/local/sbin/demo-magic.sh

export ETCDCTL_API=3
ENDPOINTS=http://localhost:2379

pe "etcdctl --endpoints=$ENDPOINTS txn -i <<EOF
mod(\"key1\") > \"0\"

put key1 \"overwrote-key1\"

put key1 \"created-key1\"
put key2 \"som extra key\"

EOF
"

pe "etcdctl --endpoints=$ENDPOINTS put key1 val1"
pei "etcdctl --endpoints=$ENDPOINTS del key1"
pei "etcdctl --endpoints=$ENDPOINTS put key2 val2"
pei "etcdctl --endpoints=$ENDPOINTS put key3 val3"
pei "etcdctl --endpoints=$ENDPOINTS get key --prefix"
