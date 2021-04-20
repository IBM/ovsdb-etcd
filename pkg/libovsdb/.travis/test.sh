#!/bin/sh -ex

rm -rf ovs
git clone --depth 1 https://github.com/openvswitch/ovs.git
cd ovs
./boot.sh && ./configure --enable-silent-rules
make -j4

srcdir=`pwd`
builddir=$srcdir
rm -rf sandbox
mkdir sandbox
sandbox=`cd sandbox && pwd`

# Below code is borrowed from OVS sandbox:
# https://github.com/openvswitch/ovs/blob/master/tutorial/ovs-sandbox

OVS_RUNDIR=$sandbox; export OVS_RUNDIR
OVS_LOGDIR=$sandbox; export OVS_LOGDIR
OVS_DBDIR=$sandbox; export OVS_DBDIR
OVS_SYSCONFDIR=$sandbox; export OVS_SYSCONFDIR
PATH=$builddir/ovsdb:$builddir/vswitchd:$builddir/utilities:$builddir/vtep:$PATH
PATH=$builddir/ovn/controller:$builddir/ovn/controller-vtep:$builddir/ovn/northd:$builddir/ovn/utilities:$PATH
export PATH

run() {
    echo "$@"
    (cd "$sandbox" && "$@") || exit 1
}

schema=$srcdir/vswitchd/vswitch.ovsschema

# Create database and start ovsdb-server.
touch "$sandbox"/.conf.db.~lock~
run ovsdb-tool create conf.db "$schema"
run ovsdb-server --detach --no-chdir --pidfile -vconsole:off --log-file -vsyslog:off \
       --remote=ptcp:6640 \
       --remote=punix:"$sandbox"/db.sock \
       --remote=db:Open_vSwitch,Open_vSwitch,manager_options

#Add a small delay to allow ovsdb-server to launch.
sleep 0.1

#Wait for ovsdb-server to finish launching.
if test ! -e "$sandbox"/db.sock; then
    printf "Waiting for ovsdb-server to start..."
    while test ! -e "$sandbox"/db.sock; do
        sleep 1;
    done
    echo "  Done"
fi

# Initialize database.
run ovs-vsctl --no-wait -- init

export GO111MODULE=on
cd ../
go get -v
go test -v
pkill ovsdb-server
rm -rf ovs
