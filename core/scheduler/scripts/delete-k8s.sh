#!/usr/bin/env bash

# 要求目标机器必须有`expect`及`Tcl`。

sysname=`uname`

# params analyzing
if [ ${sysname}='Darwin' ]; then
    # this shell only works on Mac and bash now.
    ARGS=`getopt n: $@`
elif [ ${sysname}='Linux' || ${sysname}='Unix' ]; then
    # this only works on linux/unix.
    ARGS=`getopt -o n: -l names: -- "$@"`
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

# 将所有节点的标签删除，并移除节点
if [ -n "$namesArr" ]; then
    for nodeName in ${namesArr[@]}; do
        kubectl label nodes $nodeName openwhisk-role- --overwrite && kubectl drain $nodeName --delete-local-data --ignore-daemonsets --grace-period 10 && kubectl delete node $nodeName
    done
else
    kubectl label nodes $names openwhisk-role- --overwrite && kubectl drain $names --delete-local-data --ignore-daemonsets --grace-period 10 && kubectl delete node $names
fi
