#!/bin/bash

set -x

DIR=${1:-.}
if [ -e "$DIR" ]; then
	cd $DIR
fi

mkdir -p ovn-control-plane
mkdir -p ovn-worker
mkdir -p ovn-worker2
docker cp ovn-control-plane:/var/log/openvswitch/tcpdump.pcap . 2> /dev/null
docker cp ovn-control-plane:/var/log/openvswitch/tcpdump_logs.log . 2> /dev/null
kubectl --kubeconfig=${HOME}/admin.conf get pods --all-namespaces -o wide > kube-topology
for d in ovn-control-plane ovn-worker ovn-worker2
do
    docker cp $d:/var/log/openvswitch/ovn-controller.log $d/.  
    docker cp $d:/var/log/openvswitch/ovs-vswitchd.log $d/.     
    docker cp $d:/var/log/openvswitch/ovsdb-server.log $d/.  
    docker cp $d:/var/log/ovn-kubernetes/ovn-k8s-cni-overlay.log $d/.
    docker cp $d:/var/log/ovn-kubernetes/ovnkube.log $d/.
done
d=ovn-control-plane
docker cp $d:/var/log/openvswitch/nb-ovsdb-etcd.log $d/. 
docker cp $d:/var/log/openvswitch/ovn-nbctl.log $d/. 
docker cp $d:/var/log/openvswitch/ovn-northd.log $d/.  
docker cp $d:/var/log/openvswitch/sb-ovsdb-etcd.log $d/.
docker cp $d:/var/log/ovn-kubernetes/ovnkube-master.log $d/.
