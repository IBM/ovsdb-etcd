## A jrpc client to test OVSDB implementation

In order to test the OVSDB-etcd implementation is better to use standard OVS/OVN clients, such as `ovsdb-client`, 
`ovn-nbctl` and `ovn-sbctl`. However the clients complicate to test some commands, e.g. `echo`.
This client is based on the jrpc2 package and allows sending arbitrary messages.
In addition, the client can be used for end-2-end testing. 