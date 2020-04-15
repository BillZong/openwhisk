/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.standalone

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.standalone.StandaloneDockerSupport.{containerName, createRunCmd}
import pureconfig.loadConfigOrThrow

import scala.concurrent.{ExecutionContext, Future}

class SchedulerLauncher(docker: StandaloneDockerClient, port: Int)(implicit logging: Logging,
                                                                   ec: ExecutionContext,
                                                                   actorSystem: ActorSystem,
                                                                   materializer: ActorMaterializer,
                                                                   tid: TransactionId) {

  case class SchedulerConfig(image: String)
  private val schedulerConfig = loadConfigOrThrow[SchedulerConfig](StandaloneConfigKeys.schedulerConfigKey)

  def runScheduler(wskApiHostName: String, wskApiHostPort: Int, kafkaHosts: String): Future[ServiceContainer] = {
    logging.info(this, s"Starting scheduler at $port")
    val params = Map("-p" -> Set(s"$port:8080"))
    val env = Map(
      "WHISK_API_HOST_NAME" -> wskApiHostName,
      "WHISK_API_HOST_PORT" -> wskApiHostPort.toString,
      "KAFKA_HOSTS" -> kafkaHosts)
    val name = containerName("scheduler")
    val args = createRunCmd(name, env, params)
    val f = docker.runDetached(schedulerConfig.image, args, shouldPull = false)
    val sc = ServiceContainer(port, s"scheduler is up. http://localhost:$port", s"$name")
    f.map(_ => sc)
  }
}

object SchedulerLauncher {
  val preferredSchedulerPort = 14001
}
