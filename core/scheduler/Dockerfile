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

# scheduler
FROM scala

ENV UID=1001 \
    NOT_ROOT_USER=owuser
    ENV DOCKER_VERSION=18.06.3-ce
# If you change the docker version here, it has implications on scheduler runc support.
# Docker server version and the scheduler docker version must be the same to enable runc usage.
# If this cannot be guaranteed, set `invoker_use_runc: false` in the ansible env.

# use http for ssl not working right in some districts.
RUN sed -i 's/https\:\/\/alpine.global.ssl.fastly.net/http\:\/\/dl-cdn.alpinelinux.org/g' /etc/apk/repositories

RUN apk add --update openssl openssh-client expect

ADD build/distributions/scheduler.tar ./

<<<<<<< HEAD
COPY init.sh /
COPY ali-ecs-buyer.tar.gz /
COPY ecs-buy-configs.yaml /
RUN chmod +x init.sh && tar -zxvf ali-ecs-buyer.tar.gz
=======
COPY scripts/init.sh /
RUN chmod +x init.sh
>>>>>>> 910ac224... add k8s join nodes script and renew dockerfile

COPY scripts/kubeadm scripts/kubectl /usr/bin/
COPY scripts/kube-config /root/.kube/config
COPY scripts/ecs-buyer scripts/ecs-buy-configs.yaml scripts/ecs-deleter scripts/ecs-delete-configs.yaml scripts/join-k8s.sh scripts/joiner-key.sh scripts/joiner-pwd.sh scripts/ssh-key /root/ecs/

# When running the scheduler as a non-root user this has implications on the standard directory where runc stores its data.
# The non-root user should have access on the directory and corresponding permission to make changes on it.
RUN adduser -D -u ${UID} -h /home/${NOT_ROOT_USER} -s /bin/bash ${NOT_ROOT_USER}

EXPOSE 8080
CMD ["./init.sh", "0"]
