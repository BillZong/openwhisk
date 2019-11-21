#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ -n "$2" ]; then
	if [ "$2" -eq "."] || [ "$2" -eq "./" ]; then
		PWD=$(pwd)
	else
		PWD=$2
	fi
else
	PWD=$(pwd)
fi
# fix the path
result=$(echo "$PWD" | grep "core")
if [ "$result" == "" ]; then
	PWD="$PWD/core/scheduler"
fi
result=$(echo "$PWD" | grep "scheduler")
if [ "$result" == "" ]; then
	PWD="$PWD/scheduler"
fi

echo "$PWD"

KUBE_BIN_PATH=$PWD/scripts/kube-bin
KUBE_CFG_PATH=$PWD/scripts/kube-config
NODE_HANDLER_PATH=$PWD/scripts/node-handler
OS_PLATFORM=linux/amd64

# make directory
mkdir -p $KUBE_BIN_PATH
mkdir -p $KUBE_CFG_PATH
mkdir -p $NODE_HANDLER_PATH

if [ ! -f "$KUBE_BIN_PATH/kubeadm" ]; then
	# could not use higher version than server, so we'll just use lower stable one
	# curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/$OS_PLATFORM/kubeadm
	# release on Oct 26, 2018
	curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.12.3/bin/$OS_PLATFORM/kubeadm
	chmod u+x kubeadm
	mv kubeadm $KUBE_BIN_PATH
fi


if [ ! -f "$KUBE_BIN_PATH/kubectl" ]; then
	# release on Oct 26, 2018
	curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.12.3/bin/$OS_PLATFORM/kubectl
	chmod u+x kubectl
	mv kubectl $KUBE_BIN_PATH
fi

if [ ! -f "$KUBE_CFG_PATH/config" ]; then
	echo "apiVersion: v1" > $KUBE_CFG_PATH/config
fi

if [ ! -f "$NODE_HANDLER_PATH/ssh-config" ]; then
	echo "-----BEGIN OPENSSH PRIVATE KEY-----" > $NODE_HANDLER_PATH/ssh-config
	echo "-----END OPENSSH PRIVATE KEY-----" >> $NODE_HANDLER_PATH/ssh-config
	chmod 0600 $NODE_HANDLER_PATH/ssh-config
fi

nodeJoinerURL="https://github.com/BillZong/fdn-cloud-resource-joiner/releases/download"
nodeJoinerTag="0.1.0"
nodeJoinerFiles=(join-k8s.sh joiner-key.sh joiner-pwd.sh node-joiner-configs.yaml)
nodeJoinerBinGZ=node-joiner.tar.gz
nodeJoinerBinName=node-joiner

for fileName in ${nodeJoinerFiles[@]}; do
	if [ ! -f "$NODE_HANDLER_PATH/$fileName" ]; then
		curl -LO $nodeJoinerURL/$nodeJoinerTag/$fileName
		chmod +x $fileName
		mv $fileName $NODE_HANDLER_PATH
	fi
done

if [ ! -f "$NODE_HANDLER_PATH/$nodeJoinerBinName" ]; then
	curl -LO $nodeJoinerURL/$nodeJoinerTag/$nodeJoinerBinGZ
	tar -zxf $nodeJoinerBinGZ
	mv bin/$OS_PLATFORM/$nodeJoinerBinName $NODE_HANDLER_PATH
	chmod +x $NODE_HANDLER_PATH/$nodeJoinerBinName
	# remove unsed files
	rm -f $nodeJoinerBinGZ
	rm -rf bin
fi

nodeDeleterURL="https://github.com/BillZong/fdn-cloud-resource-deleter/releases/download"
nodeDeleterTag="0.1.0"
nodeDeleterFiles=(delete-k8s.sh deleter-key.sh deleter-pwd.sh node-deleter-configs.yaml)
nodeDeleterBinGZ=node-deleter.tar.gz
nodeDeleterBinName=node-deleter

for fileName in ${nodeDeleterFiles[@]}; do
	if [ ! -f "$NODE_HANDLER_PATH/$fileName" ]; then
		curl -LO $nodeDeleterURL/$nodeDeleterTag/$fileName
		chmod +x $fileName
		mv $fileName $NODE_HANDLER_PATH
	fi
done

if [ ! -f "$NODE_HANDLER_PATH/$nodeDeleterBinName" ]; then
	curl -LO $nodeDeleterURL/$nodeDeleterTag/$nodeDeleterBinGZ
	tar -zxf $nodeDeleterBinGZ
	mv bin/$OS_PLATFORM/$nodeDeleterBinName $NODE_HANDLER_PATH
	chmod +x $NODE_HANDLER_PATH/$nodeDeleterBinName
	# remove unsed files
	rm -f $nodeDeleterBinGZ
	rm -rf bin
fi
