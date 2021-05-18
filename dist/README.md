# Deplyment of ovsdb-etcd on OVN kubernetes

In order to deploy [OVN kubernetes](https://github.com/ovn-org/ovn-kubernetes) with ovsdb-etcd follow the following steps:

## Build ovn

```bash
git clone https://github.com/hunchback/ovn
cd ovn
git checkout ovndb-etcd
./boot.sh
./configure
make
export OVN_ROOT=$PWD
```

## Prepare ovs-kubernetes

Clone OVN kubernetes from the main repository:

```bash
# Clone ovn-kubernetes repo
git clone https://github.com/hunchback/ovn-kubernetes
cd ovn-kubernetes
git checkout ovndb-etcd
export OVN_KUBERNETES_ROOT=$PWD
		```
Build the OVN kubernetes container image:

```bash
make -C go-controller
```

```bash
cp $OVN_ROOT/utilities/ovn-nbctl dist/images/
make -C dist/images fedora
```

## Build ovn-kubernetes

clone and build and push (aboute 5 minutes):

```bash
git clone https://github.com/IBM/ovsdb-etcd
cd ovsdb-etcd
make ovnk-build
```

## Push ovn-kubernetes

Login to docker:

```
export CR_PASSWORD=<DOCKER_PASSWORD>
export CR_USERNAME=<DOCKER_USENAME>
export CR_REPO=docker.io
echo $CR_PAT | docker login $CR_REPO -u $CR_USERNAME --password-stdin
export OVSDB_ETCD_REPOSITORY=$CR_REPO/$CR_USERNAME
```

Push to repo:

```bash
make ovnk-push
```

## Deploy ovn-kubernetes

Deploy kind (for more information read [OVN kubernetes KIND deployment official documentation](https://github.com/ovn-org/ovn-kubernetes/blob/master/docs/kind.md))

```bash
make ovnk-deploy
```

After the deplyment is complete you can check out the relevant pods.

```bash
make ovnk-status
```
