## Running ovn-kubernetes with ovsdb-etcd

In order to run [ovn-kubernetes](https://github.com/ovn-org/ovn-kubernetes) with ovsdb-etcd:
* build the ovsdb-etc image and push it to a common image repository e.g. docker hub or quay
* update the image name in the ovn-kubernetes/dist/templates/ovnkube-db.yaml.j2 (will be set by kind.sh arguments)
* replace OVNk files by corresponded files from the ovn-kubernetes directory.
* run kind.sh to deploy ovn-kubernetes 