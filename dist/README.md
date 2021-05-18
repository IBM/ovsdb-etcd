# Deplyment of ovsdb-etcd on OVN kubernetes
In order to deploy [OVN kubernetes](https://github.com/ovn-org/ovn-kubernetes) with ovsdb-etcd follow the following steps:

1. prepare OVN kubernetes (for more information read [OVN kubernetes KIND deployment official documentation](https://github.com/ovn-org/ovn-kubernetes/blob/master/docs/kind.md))
	- clone OVN kubernetes from the main repository.
		```bash
		# Clone ovn-kubernetes repo
		git clone https://github.com/ovn-org/ovn-kubernetes
		cd ovn-kubernetes
		git checkout d0fdcfbbb2702ed8482a0c1f6ba4561273399fdc
		```
	- build the OVN kubernetes container image.
		```bash
		pushd go-controller
		make
		popd
		pushd dist/images
		make fedora
		popd
		```
1. prepare ovsdb-etcd
	- clone `ovsdb-etcd` from the main repository.
		```bash
		# Clone ovsdb-etcd repo
		git clone https://github.com/IBM/ovsdb-etcd
		cd ovsdb-etcd
		```
	- login to your docker hub account.
	- set the environment variable `OVSDB_ETCD_REPOSITORY` with your docker.io image repository name. for example:
		```bash
		export OVSDB_ETCD_REPOSITORY=omerbh
		```
	- set the environment variable `OVN_KUBERNETES_ROOT` to the location of OVN kubernetes repository. for example:
		```bash
		export OVN_KUBERNETES_ROOT=$HOME/go/src/github.com/ovn-org/ovn-kubernetes
		```
	    (default value : `<this repository root directory>/../../ovn-org/ovn-kubernetes`)
	- run 
		```bash
		make ovn-kubernetes-build
		```
1. deploy kind (for more information read [OVN kubernetes KIND deployment official documentation](https://github.com/ovn-org/ovn-kubernetes/blob/master/docs/kind.md))
	- `cd` to the OVN kubernetes repository.
	- run `kind.sh` to deploy OVN kubernetes with ovsdb-etcd.
		```bash
		 pushd contrib                                                                                                        
		 KUBECONFIG=${HOME}/admin.conf
		 ./kind.sh
		 popd
		```

1. after the deplyment is complete you can check out the relevant pods.
	```bash
	kubectl --kubeconfig=$KUBECONFIG -n=ovn-kubernetes get pods
	```
The ovn-kubernetes files are from commit d0fdcfbbb2702ed8482a0c1f6ba4561273399fdc
