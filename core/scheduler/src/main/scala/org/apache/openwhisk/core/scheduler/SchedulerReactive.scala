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
import org.apache.openwhisk.core.entity.{InvokerNodeLimit, SchedulerInstanceId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.spi.SpiLoader

import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import sys.process._

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
  val minNodesCfg = InvokerNodeLimit.config.min
  val maxNodesCfg = InvokerNodeLimit.config.max
  var currentNodesCount = 0 // TODO: race condition problem

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  private val resourcePool = Map("memory" -> 0.toLong)

  // initialize msg consumer
  val msgProvider = SpiLoader.get[MessagingProvider]
  logging.info(this, s"create instance, id: ${instance.asString}")
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

  // when handling, could not handle more info again.
  private var buying: Boolean = false // TODO: race condition problem
  private var deleting: Boolean = false // TODO: race condition problem

  private var avgResourcePercentage = 0L

  /** Is called when an resource report is read from Kafka */
  def processResourceMessage(bytes: Array[Byte]): Future[Unit] = {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    Future(Metric.parse(raw))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        logging.info(this, s"scheduler${instance.asString} got resource msg: ${msg}")

        msg.metricName match {
          case "memoryUsedPercentage" =>
            avgResourcePercentage = msg.metricValue
            resourceFeed ! MessageFeed.Processed
          case "memoryTotal" =>
            resourcePool("memoryTotal") = msg.metricValue // MB
            resourceFeed ! MessageFeed.Processed
          case "OnlineInvokerCount" =>
            currentNodesCount = msg.metricValue.toInt // 在线的Invoker数量
            resourceFeed ! MessageFeed.Processed // block the queue
          case "slotsNotEnough" =>
//            resourceFeed ! MessageFeed.Processed // don't block the queue
            // TODO: 3 times check
            if (!buying) {
              buying = true
              var buyCount = 5
              if (buyCount + currentNodesCount > maxNodesCfg) {
                buyCount = maxNodesCfg - currentNodesCount
              }
              val proc = Process(s"/root/ecs/ecs-buyer -c /root/ecs/ecs-buy-configs.yaml --node-count ${buyCount}")
              val ret = proc.run()
              if (ret.exitValue == 0) {
                logging.info(this, s"buying ecs success, ${buyCount} nodes added")
              } else {
                logging.info(this, s"buying ${buyCount} nodes ecs failed: ${ret.exitValue}")
              }
              buying = false
            } else {
              logging.info(this, s"buying nodes now, could not handle more of it.")
            }
            resourceFeed ! MessageFeed.Processed // block the queue
          case "slotsTooMuch" =>
            // TODO: 3 times check
            if (!deleting) {
              deleting = true
              var deleteCount = 5
              if (currentNodesCount - deleteCount > minNodesCfg) {
                deleteCount = currentNodesCount - minNodesCfg
              }
              val proc =
                Process(s"/root/ecs/ecs-deleter -c /root/ecs/ecs-delete-configs.yaml --node-count ${deleteCount}")
              Process(s"/bin/linux/arm64/ali-ecs-deleter -c /ecs-delete-configs.yaml --node-count ${deleteCount}")
              val ret = proc.run()
              if (ret.exitValue == 0) {
                logging.info(this, s"delete ecs success, ${deleteCount} nodes deleted")
              } else {
                logging.info(this, s"deleting ${deleteCount} nodes ecs failed: ${ret.exitValue}")
              }
              deleting = false
            } else {
              logging.info(this, s"deleting nodes now, could not handle more of it.")
            }
            resourceFeed ! MessageFeed.Processed // block the queue
        }

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
