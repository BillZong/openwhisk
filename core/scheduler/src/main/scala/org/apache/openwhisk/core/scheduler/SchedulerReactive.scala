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
import akka.http.scaladsl.model.DateTime
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.entity.SchedulerInstanceId
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.spi.SpiLoader
import pureconfig.loadConfigOrThrow

import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import sys.process._

// config case class
case class InvokerNodeLimitConfig(min: Int, max: Int)
case class ResourceDrainThreshold(minPercentage: Int, duration: Int)

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

  // invoker node count limits
  val invokerNodeLimits = loadConfigOrThrow[InvokerNodeLimitConfig]("whisk.scheduler.invoker-nodes")
  var currentNodesCount = 0 // TODO: race condition problem

  // resource draining threshold
  val resourceDrainCfg = loadConfigOrThrow[ResourceDrainThreshold]("whisk.scheduler.resource-drain-threshold")
  logging.info(this, s"resourceDrainCfg: $resourceDrainCfg")
  private var avgResourcePercentage = resourceDrainCfg.minPercentage
  private var handlingUnderThreshold = false
  private var lastCountTime = DateTime.now // milliseconds

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  private val resourcePool = Map("memory" -> 0.toLong)

  // initialize msg consumer
  val msgProvider = SpiLoader.get[MessagingProvider]
  logging.info(this, s"create instance, id: ${instance.asString}")
  val msgConsumer = msgProvider.getConsumer(config, s"scheduler${instance.asString}", "resource", 4096)
  val pingPollDuration = 1.second
  val resourceFeed: ActorRef = actorSystem.actorOf(Props {
    new MessageFeed(
      "resource",
      logging,
      msgConsumer,
      msgConsumer.maxPeek,
      pingPollDuration,
      processResourceMessage,
      logHandoff = false)
  })

  // when handling, could not handle more info again.
  private var buying: Boolean = false // TODO: race condition problem
  private var deleting: Boolean = false // TODO: race condition problem

  override def processResourceMessage(bytes: Array[Byte]): Future[Unit] = {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    Future(Metric.parse(raw))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        logging.info(this, s"scheduler${instance.asString} got resource msg: ${msg}")
        var retVal = 0

        msg.metricName match {
          case "memoryUsedPercentage" =>
            avgResourcePercentage = msg.metricValue.toInt
            if (!handlingUnderThreshold && avgResourcePercentage < resourceDrainCfg.minPercentage) {
              // under threshold
              handlingUnderThreshold = true
              val now = DateTime.now
              if ((lastCountTime + resourceDrainCfg.duration * 1000).compare(now) < 0) { // exceed a setting time
                logging.info(this, "resource is under threshold, try to delete nodes")
                lastCountTime = now
                // handle msg
                processResourceMessage(
                  Metric("slotsTooMuch", msg.metricValue).serialize.getBytes(StandardCharsets.UTF_8))
              }
              handlingUnderThreshold = false
            } else {
              logging.info(this, "deleting nodes, would not handle this info")
            }
            resourceFeed ! MessageFeed.Processed
          case "memoryTotal" =>
            resourcePool("memoryTotal") = msg.metricValue // MB
            resourceFeed ! MessageFeed.Processed
          case "OnlineInvokerCount" =>
            currentNodesCount = msg.metricValue.toInt // 在线的Invoker数量
            resourceFeed ! MessageFeed.Processed // block the queue
          case "slotsNotEnough" =>
            // resourceFeed ! MessageFeed.Processed // don't block the queue
            // TODO: 3 times check
            if (!buying) {
              buying = true
              var buyCount = 1
              if (buyCount + currentNodesCount > invokerNodeLimits.max) {
                buyCount = invokerNodeLimits.max - currentNodesCount
              }
              val proc = Process(s"/root/ecs/ecs-buyer -c /root/ecs/ecs-buy-configs.yaml --node-count ${buyCount}")
              val ret = proc.run()
              retVal = ret.exitValue
              if (retVal == 0) {
                logging.info(this, s"buying ecs success, ${buyCount} nodes added")
              } else {
                logging.error(this, s"buying ${buyCount} nodes ecs failed: ${ret.exitValue}")
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
              var deleteCount = 1
              if (currentNodesCount - deleteCount > invokerNodeLimits.min) {
                deleteCount = currentNodesCount - invokerNodeLimits.min
              }
              val proc =
                Process(s"/root/ecs/ecs-deleter -c /root/ecs/ecs-delete-configs.yaml --node-count ${deleteCount}")
              val ret = proc.run()
              retVal = ret.exitValue
              if (retVal == 0) {
                logging.info(this, s"delete ecs success, ${deleteCount} nodes deleted")
              } else {
                logging.error(this, s"deleting ${deleteCount} nodes ecs failed: ${ret.exitValue}")
              }
              deleting = false
            } else {
              logging.info(this, s"deleting nodes now, could not handle more of it.")
            }
            resourceFeed ! MessageFeed.Processed // block the queue
        }
        if (retVal == 0) {
          Future.successful(())
        } else {
          Future.failed(throw new Exception(s"handle ecs nodes failed: ${retVal}"))
        }
      }
      .recoverWith {
        case t =>
          resourceFeed ! MessageFeed.Processed
          logging.error(this, s"failed processing message: $raw with $t")
          Future.successful(())
      }
  }
}
