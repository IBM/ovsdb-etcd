#!/usr/bin/env bash

set -x

export KUBECONFIG=${HOME}/admin.conf

run_kubectl() {
  local retries=0
  local attempts=3
  while true; do
    if kubectl "$@"; then
      break
    fi

    ((retries += 1))
    if [[ "${retries}" -gt ${attempts} ]]; then
      echo "error: 'kubectl $*' did not succeed, failing"
      exit 1
    fi
    echo "info: waiting for 'kubectl $*' to succeed..."
    sleep 1
  done
}

get_pods() {
	run_kubectl -n=ovn-kubernetes get pods -o wide
}

get_pods_list() {
	run_kubectl -n=ovn-kubernetes get pods| tail -n +2 |cut -d " " -f1
}

#can be optimized with get_pods_listget_pods_list
get_only_ovsdb_etcd_pods_list() {
	run_kubectl -n=ovn-kubernetes get pods| tail -n +2 |cut -d " " -f1 | grep ovnkube-db
}

get_container_list() {
	run_kubectl -n=ovn-kubernetes get pod $1 -o jsonpath={.spec.containers[*].name}
}

describe_pod() {
	run_kubectl -n=ovn-kubernetes describe pod $1
}

get_logs_from_container_in_a_pod() {
	:'
	$1 is pod name
	$2 is container name
	'
	run_kubectl -n=ovn-kubernetes logs $1 $2
}

#can be optimized with exec_from_continer_in_a_pod
running_tasks_of_continer_in_a_pod() {
	:'
	$1 is pod name
	$2 is container name
	'
	run_kubectl -n=ovn-kubernetes exec $1 -c $2 -- ps -ef
}

#can be optimized with exec_from_continer_in_a_pod
get_yaml_of_a_pod() {
	:'
	$1 is pod name
	'
	run_kubectl -n=ovn-kubernetes get pod $1 -o yaml
}

#can be optimized with exec_from_continer_in_a_pod
get_json_of_a_pod() {
	:'
	$1 is pod name
	'
	run_kubectl -n=ovn-kubernetes get pod $1 -o json
}

#can be optimized with exec_from_continer_in_a_pod
log_file_names() {
	:'
	$1 is ovsdb-etcd pod name
	$2 is container name
	'
	run_kubectl -n=ovn-kubernetes exec $1 -c $2 -- ls /var/log/ovn
}

exec_from_continer_in_a_pod() {
	:'
	$1 is pod name
	$2 is container name
	$3 is command
	'
	run_kubectl -n=ovn-kubernetes exec $1 -c $2 -- "$3"
}

:'
copy_files_from_ovsdb_etcd_pod() {
	#$1 is pod name
}
'

dir_name=$(eval date +'%-d-%-m-%Y_%-H-%M-%S')
root_path=${PWD}/logs/"$dir_name"
mkdir $root_path
echo "$(eval get_pods)" > $root_path/get_pods.txt 2>&1
for POD_NAME in $(eval get_pods_list)
do
		pod_path=$root_path/"$POD_NAME"
		mkdir $pod_path
		echo "$(eval describe_pod $POD_NAME)" > $pod_path/describe_pod.txt 2>&1
		echo "$(eval get_yaml_of_a_pod $POD_NAME)" > $pod_path/pod_yaml.yaml 2>&1
		echo "$(eval get_json_of_a_pod $POD_NAME)" > $pod_path/pod_json.json 2>&1
		for CONTAINER_NAME in $(eval get_container_list $POD_NAME)
		do
				container_path=$pod_path/"$CONTAINER_NAME"
				mkdir $container_path
				echo "$(eval get_logs_from_container_in_a_pod $POD_NAME $CONTAINER_NAME)" > $container_path/logs.txt 2>&1
				echo "$(eval running_tasks_of_continer_in_a_pod $POD_NAME $CONTAINER_NAME)" > $container_path/ps_ef.txt 2>&1
		done
done

for OVNKUBE_POD_NAME in $(eval get_only_ovsdb_etcd_pods_list)
do
		pod_path=$root_path/"$OVNKUBE_POD_NAME"
		local_ovsdb_log_pod_path="$pod_path"/ovsdb_logs
		echo $local_ovsdb_log_pod_path
		mkdir -p $local_ovsdb_log_pod_path
		remote_log_folder=/var/log/ovn
		log_files_names=$(eval run_kubectl -n=ovn-kubernetes exec $OVNKUBE_POD_NAME -c etcd -- ls $remote_log_folder)
		echo $log_files_names
		for log_file in $log_files_names
		do
				remote_log_file_full_name=$remote_log_folder/$log_file
				local_log_file_path=$local_ovsdb_log_pod_path/$log_file
				$(eval run_kubectl -n=ovn-kubernetes cp $OVNKUBE_POD_NAME:$remote_log_file_full_name  -c etcd $local_log_file_path)
				echo $log_file_full_name
		done
done
