#!/usr/bin/env bash

export KUBECONFIG=${HOME}/admin.conf
rm -rf ovn-control-plane
rm -rf ovn-worker
rm -rf ovn-worker2
mkdir -p ovn-control-plane
mkdir -p ovn-worker
mkdir -p ovn-worker2
docker cp ovn-control-plane:/var/log/openvswitch/tcpdump.pcap . 2>/dev/null
docker cp ovn-control-plane:/var/log/openvswitch/tcpdump_logs.log . 2>/dev/null
kubectl get pods --all-namespaces -o wide > kube-topology  2>/dev/null
for d in ovn-control-plane ovn-worker ovn-worker2
do
	docker cp $d:/var/log/openvswitch/ovn-controller.log $d/. 2>/dev/null
	docker cp $d:/var/log/openvswitch/ovs-vswitchd.log $d/. 2>/dev/null
	docker cp $d:/var/log/openvswitch/ovsdb-server.log $d/. 2>/dev/null
	docker cp $d:/var/log/ovn-kubernetes/ovn-k8s-cni-overlay.log $d/. 2>/dev/null
	docker cp $d:/var/log/ovn-kubernetes/ovnkube.log $d/. 2>/dev/null
	docker cp $d:/var/log/openvswitch/nb-ovsdb-etcd.log $d/. 2>/dev/null
	docker cp $d:/var/log/openvswitch/ovn-nbctl.log $d/. 2>/dev/null
	docker cp $d:/var/log/openvswitch/ovn-northd.log $d/. 2>/dev/null
	docker cp $d:/var/log/openvswitch/sb-ovsdb-etcd.log $d/. 2>/dev/null
	docker cp $d:/var/log/ovn-kubernetes/ovnkube-master.log $d/. 2>/dev/null
done
