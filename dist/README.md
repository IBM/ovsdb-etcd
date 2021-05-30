# Deplyment of ovsdb-etcd on OVN kubernetes


## Prepare ovn-kubernetes

Clone OVN kubernetes from the main repository:

```bash
git clone https://github.com/ibm/ovn-kubernetes
pushd ovn-kubernetes
git checkout ovsdb-etcd2
export OVN_KUBERNETES_ROOT=$PWD
popd
		```
## Build docker images

clone and build and push:

```bash
git clone https://github.com/IBM/ovsdb-etcd
pushd ovsdb-etcd
make docker-build
popd
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
pushd ovsdb-etcd
make ovnk-deploy
popd
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
pushd ovsdb-etcd
make ovnk-status
popd
```

Or directly:

```bash
export KUBECONFIG=${HOME}/admin.conf
kubectl -n=ovn-kubernetes get pods
```
