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

package org.apache.openwhisk.core.scheduler

import java.nio.charset.StandardCharsets

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.spi.SpiLoader

import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object SchedulerReactive extends SchedulerProvider {

  override def instance(config: WhiskConfig, instance: SchedulerInstanceId, producer: MessageProducer)(
    implicit actorSystem: ActorSystem,
    logging: Logging): SchedulerCore =
    new SchedulerReactive(config, instance, producer)
}

class SchedulerReactive(config: WhiskConfig, instance: SchedulerInstanceId, producer: MessageProducer)(
  implicit actorSystem: ActorSystem,
  logging: Logging)
    extends SchedulerCore {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val cfg: WhiskConfig = config

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  private val resourcePool = Map("memory" -> 0.toLong)

  // initialize msg consumer
  val msgProvider = SpiLoader.get[MessagingProvider]
  val testConsumer = msgProvider.getConsumer(config, s"scheduler${instance.asString}", "resource", 1024)
  val pingPollDuration = 1.second
  val resourceFeed: ActorRef = actorSystem.actorOf(Props {
    new MessageFeed(
      "resourceTotal",
      logging,
      testConsumer,
      testConsumer.maxPeek,
      pingPollDuration,
      processResourceMessage,
      logHandoff = false)
  })

  /** Is called when an resource report is read from Kafka */
  def processResourceMessage(bytes: Array[Byte]): Future[Unit] = {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    Future(Metric.parse(raw))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        msg.metricName match {
          case "memoryTotal" =>
            resourcePool("memoryTotal") = msg.metricValue // MB
        }

        resourceFeed ! MessageFeed.Processed

        logging.info(this, s"scheduler${instance.asString} got resource msg: ${msg}")
        Future.successful(())
      }
      .recoverWith {
        case t =>
          resourceFeed ! MessageFeed.Processed
          logging.error(this, s"failed processing message: $raw with $t")
          Future.successful(())
      }
  }
}
