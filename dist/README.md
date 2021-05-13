## Running ovn-kubernetes with ovsdb-etcd

In order to run [ovn-kubernetes](https://github.com/ovn-org/ovn-kubernetes) with ovsdb-etcd:
* set the environment variable `OVSDB_ETCD_REPOSITORY` with your image repository name
* optionally set `OVN_KUBERNETES_ROOT` to location of ovn-kubernetes repository, 
  default is <this repository>/./../../ovn-org/ovn-kubernetes )  
* run `make ovn-kubernetes-build` 
* run kind.sh to deploy ovn-kubernetes 

The ovn-kubernetes files are from commit e567a39c76930a26bdf1f312c36fa716f18df9a7