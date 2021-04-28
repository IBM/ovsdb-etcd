#!/bin/sh
while getopts d:c:p: flag
do
		case "${flag}" in
				d) data_dir=${OPTARG};;
				c) client_urls_port=${OPTARG};;
				p) peer_urls_port=${OPTARG};;
		esac
done
echo "running etcd server with the following parameters"
echo "data dir: $data_dir";
echo "client urls: $client_urls_port";
echo "peer_urls: $peer_urls_port";

etcd \
	 --data-dir=$data_dir \
	 --listen-peer-urls "http://localhost:$peer_urls_port" \
	 --listen-client-urls "http://localhost:$client_urls_port" \
	 --initial-advertise-peer-urls "http://localhost:$peer_urls_port" \
	 --initial-cluster "default=http://localhost:$peer_urls_port" \
	 --advertise-client-urls "http://localhost:${client_urls_port}"
