# Deplyment of ovsdb-etcd on OVN kubernetes


## Prepare ovs-kubernetes

Clone OVN kubernetes from the main repository:

```bash
git clone https://github.com/hunchback/ovn-kubernetes
cd ovn-kubernetes
git checkout ovsdb-etcd2
export OVN_KUBERNETES_ROOT=$PWD
		```
## Build docker images

clone and build and push:

```bash
git clone https://github.com/IBM/ovsdb-etcd
cd ovsdb-etcd
make docker-build
```

## Setup docker paramaters

```
export CR_PASSWORD=<DOCKER_PASSWORD>
export CR_USERNAME=<DOCKER_USENAME>
export CR_REPO=docker.io
export OVSDB_ETCD_REPOSITORY=$CR_USERNAME
```

## Push docker images (optional)

Login to docker and push:

```
echo $CR_PAT | docker login $CR_REPO -u $CR_USERNAME --password-stdin
make docker-push
```

## Deploy ovn-kubernetes

Deploy on kind via makefile:

```bash
make ovn-deploy
```

Or directly:

```bash
export KUBECONFIG=${HOME}/admin.conf
pushd ${OVN_KUBERNETES_ROOT}/contrib
./kind.sh \
	--ovn-etcd-image "${OVSDB_ETCD_REPOSITORY}/etcd:latest" \
	--ovn-ovsdb-etcd-image "${OVSDB_ETCD_REPOSITORY}/ovsdb-etcd:latest" \
	--master-loglevel 7 \
	--node-loglevel 7 \
	--dbchecker-loglevel 7 \
	--ovn-loglevel-northd '-vconsole:dbg -vfile:dbg' \
	--ovn-loglevel-nbctld '-vconsole:dbg -vfile:dbg' \
	--ovn-loglevel-controller '-vconsole:dbg -vfile:dbg'
popd
```
## Get status ovn-kubernetes

Check status via makefile:

```bash
make ovn-status
```

Or directly:

```bash
export KUBECONFIG=${HOME}/admin.conf
kubectl --kubeconfig=${KUBECONFIG} -n=ovn-kubernetes get pods
```
