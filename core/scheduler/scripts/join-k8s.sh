#!/usr/bin/env bash

# 要求目标机器必须有`expect`及`Tcl`。

sysname=`uname`

# params analyzing
if [ ${sysname}='Darwin' ]; then
    # this shell only works on Mac and bash now.
    ARGS=`getopt h:n:u:p:P:s: $@`
elif [ ${sysname}='Linux' || ${sysname}='Unix' ]; then
    # this only works on linux/unix.
    ARGS=`getopt -o h:n:u:p:P:s: -l hosts:,names:,user:,password:,port:ssh-file: -- "$@"`
else
    echo "Windows not supported yet"
fi

if [ $? != 0 ]; then
    echo "Terminating..."
    exit 1
fi

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

sep=","
while true
do
    case "${1}" in
        -h|--hosts)
            hosts="$2"
            result=$(echo $2 | grep ",")
    	    if [ "$result" != "" ]; then
    	        OLD_IFS=$IFS
    	        IFS=","
    	        hostsArr=($hosts)
    	        IFS=$OLD_IFS
    	    fi
            shift 2
            ;;
        -n|--names)
            names=$2
            result=$(echo $2 | grep ",")
            if [ "$result" != "" ]; then
                OLD_IFS="$IFS"
                IFS=","
                namesArr=($names)
                IFS="$OLD_IFS"
            fi
            shift 2
            ;;
        -u|--user)
            user=$2
            shift 2
            ;;
        -p|--password)
            password=$2
            shift 2
            ;;
        -P|--port)
            port=$2
            shift 2
            ;;
        -s|--ssh-file)
            sshFile=$2
            shift 2
            ;;
        --)
            shift;
            break;
            ;;
        *)
	    echo "Internal error!"
            exit 1
            ;;
    esac
done

if [ -z $user ]; then
    user="root"
fi

if [ -n $sshFile ]; then
    join_file="./joiner-key.sh"
    key=$sshFile
elif [ -n $password ]; then
    join_file="./joiner-pwd.sh"
    key=$password
else
    echo "no ssh key file and password, could not login"
    exit 1
fi

if [ -z $port ]; then
    port=22
fi

join_token_cmd_file=./token.txt
# 在当前宿主机生成临时用token
kubeadm token create --ttl 5m --print-join-command >$join_token_cmd_file
# echo "kubeadm join 172.18.139.54:6443 --token cthmyb.ep7g3reaa024gus5 --discovery-token-ca-cert-hash sha256:6b721270f1b519dbc1f739ec4d7b37b2edb387b795e35a9b8faede8192f0822a" > $join_token_cmd_file

# ssh 所有节点，并将其加入K8S集群
if [ -n "$hostsArr" ]; then
    for host in ${hostsArr[@]}; do
        $join_file $host $port $user $key $join_token_cmd_file
    done
else
    $join_file $hosts $port $user $key $join_token_cmd_file
fi

# 将每个节点都打上标签
if [ -n "$namesArr" ]; then
    for nodeName in ${namesArr[@]}; do
        kubectl label nodes $nodeName openwhisk-role=invoker --overwrite
    done
else
    kubectl label nodes $names openwhisk-role=invoker --overwrite
fi

# 移除token文件
rm -f $join_token_cmd_file
