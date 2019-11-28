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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import sys.process._

// config case class
case class InvokerNodeLimitConfig(min: Int, max: Int)
case class ResourceDrainThreshold(minPercentage: Int, duration: Int)
case class NodeHandlerBinaryConfig(bin: String, cfg: String)

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

  val nodeJoinerConfig = loadConfigOrThrow[NodeHandlerBinaryConfig]("whisk.scheduler.node-handler-binary.joiner")
  val nodeDeleterConfig = loadConfigOrThrow[NodeHandlerBinaryConfig]("whisk.scheduler.node-handler-binary.deleter")

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

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
  private var nodeHandling: Boolean = false // TODO: might be race condition problem?

  // store for history data, in order to count nodes to join/delete
  private var totalMemory = 0
  private case class SlotStore(msg: Metric, time: DateTime)
  private case class UsageStore(usage: Int, time: DateTime)
  private val slotStores: ArrayBuffer[SlotStore] = new ArrayBuffer[SlotStore]
  private val usageStores: ArrayBuffer[UsageStore] = new ArrayBuffer[UsageStore]

  override def processResourceMessage(bytes: Array[Byte]): Future[Unit] = {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    Future(Metric.parse(raw))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        logging.info(this, s"scheduler${instance.asString} got resource msg: ${msg}")
        var ret = (0, "")
        msg.metricName match {
          case "memoryUsedPercentage" =>
            handlerMemoryUsageMessage(msg.metricValue.toInt)
            resourceFeed ! MessageFeed.Processed
          case "memoryTotal" =>
            totalMemory = msg.metricValue.toInt // MB
            resourceFeed ! MessageFeed.Processed
          case "OnlineInvokerCount" =>
            currentNodesCount = msg.metricValue.toInt // 在线的Invoker数量
            resourceFeed ! MessageFeed.Processed // block the queue
          case "slotsNotEnough" =>
            ret = handleNotEnoughMessage(msg)
            resourceFeed ! MessageFeed.Processed // block the queue
          case "slotsTooMuch" =>
            ret = handleTooMuchMessage(msg.metricValue.toInt)
            resourceFeed ! MessageFeed.Processed // block the queue
        }
        if (ret._1 == 0) Future.successful(()) else Future.failed(throw new Exception(ret._2))
      }
      .recoverWith {
        case t =>
          resourceFeed ! MessageFeed.Processed
          logging.error(this, s"failed processing message: $raw with $t")
          Future.successful(())
      }
  }

  private def handlerMemoryUsageMessage(percent: Int): Unit = {
    avgResourcePercentage = percent

    if (avgResourcePercentage >= resourceDrainCfg.minPercentage) {
      // once exceed the limit, postpone the deleting process
      lastCountTime = DateTime.now
      usageStores.clear()
      return
    }
    if (handlingUnderThreshold) {
      logging.info(this, "deleting nodes, would not handle this info")
      return
    }
    // under threshold
    val now = DateTime.now
    usageStores += UsageStore(avgResourcePercentage, now)
    if ((lastCountTime + resourceDrainCfg.duration * 1000).compare(now) > 0) { // within setting limit time
      return
    }

    handlingUnderThreshold = true
    logging.info(this, "resource is under threshold, try to delete nodes")
    lastCountTime = now

    // check the average fix out how many nodes should delete
    val calcValue = usageStores.map(_.usage)
    val avg = calcValue.sum.toFloat / calcValue.length
    var ratio = avg / avgResourcePercentage
    if (ratio > 1.0) {
      ratio = 1.0.toFloat // max ratio
    } else if (ratio < 0.2) {
      ratio = 0.2.toFloat // min ratio
    }
    val deleteNodeCount = Math.ceil(currentNodesCount * (1 - ratio)).toLong

    // handle msg
    processResourceMessage(Metric("slotsTooMuch", deleteNodeCount).serialize.getBytes(StandardCharsets.UTF_8))
    handlingUnderThreshold = false
  }

  private def handleNotEnoughMessage(msg: Metric): (Int, String) = {
    if (nodeHandling) {
      logging.info(this, s"handling nodes now, could not handle more of it.")
      return (0, "")
    }

    slotStores += SlotStore(msg, DateTime.now)

    val slotsPerNode = totalMemory / currentNodesCount
    val totalNodeMemory = totalMemory
    val totalNodeCount = currentNodesCount
    //check msg from the same component for n times
    var checkCount = Math.ceil(Math.sqrt(slotsPerNode / msg.metricValue)).toInt
    if (checkCount > 5) {
      checkCount = 5 // max check count
    }
    if (slotStores.length < checkCount) {
      return (0, "")
    }
    val filterStores = slotStores.filter(_.msg.transid == msg.transid)
    if (filterStores.length < checkCount) {
      logging.debug(this, s"the not enough slot message from the same loadbalancer count less than $checkCount")
      return (0, "")
    }

    val avgSlots = filterStores.map(_.msg.metricValue.toInt).sum / filterStores.length
    val durations = for ((store, idx) <- filterStores.view.zipWithIndex if (idx < filterStores.length - 1))
      yield filterStores(idx + 1).time.clicks - store.time.clicks
    val avgMilliSeconds = durations.sum / durations.length
    // count number in 1 seconds
    val rps = 1000.toFloat / avgMilliSeconds
    var ratio = rps * avgSlots / totalNodeMemory
    if (rps < 1.0 || Math.round(ratio * 100) < resourceDrainCfg.minPercentage) {
      logging.debug(this, "the requests could be handled, no need to add nodes")
      return (0, "")
    }

    if (ratio > 1.2) {
      ratio = 1.2.toFloat // max ratio
    }
    var joinCount = Math.ceil(ratio * totalNodeCount).toInt

    if (joinCount + currentNodesCount > invokerNodeLimits.max) {
      joinCount = invokerNodeLimits.max - currentNodesCount
    }
    if (joinCount <= 0) {
      logging.info(this, "cluster nodes reach max, no nodes could be joined now")
      return (0, "")
    }

    var retVal = 0
    var exceptionMsg = ""

    nodeHandling = true
    try {
      val proc = Process(s"${nodeJoinerConfig.bin} -c ${nodeJoinerConfig.cfg} --node-count ${joinCount}")
      val ret = proc.run()
      retVal = ret.exitValue
      if (retVal == 0) {
        logging.info(this, s"joining nodes success, ${joinCount} nodes added")
      } else {
        exceptionMsg = "joining node scripts failed: see the log"
        logging.error(this, s"joining ${joinCount} nodes failed: ${ret.exitValue}")
      }
    } catch {
      case e: Exception =>
        retVal = -1
        exceptionMsg = e.getMessage
        logging.error(this, s"joining ${joinCount} nodes failed with exception: ${e.getMessage}")
    }
    slotStores.clear() // clear slots info, for handler another message again
    nodeHandling = false

    (retVal, exceptionMsg)
  }

  private def handleTooMuchMessage(nodeCount: Int): (Int, String) = {
    if (nodeCount <= 0) {
      logging.warn(this, s"cluster delete node count invalid: $nodeCount")
      return (0, "")
    }

    if (nodeHandling) {
      logging.info(this, s"handling nodes now, could not handle more of it.")
      return (0, "")
    }

    var deleteCount = nodeCount
    if (currentNodesCount - nodeCount < invokerNodeLimits.min) {
      deleteCount = currentNodesCount - invokerNodeLimits.min
    }
    if (nodeCount <= 0) {
      logging.info(this, "cluster nodes reach min, no nodes could be deleted now")
      return (0, "")
    }

    var retVal = 0
    var exceptionMsg = ""

    nodeHandling = true
    try {
      val proc =
        Process(s"${nodeDeleterConfig.bin} -c ${nodeDeleterConfig.cfg} --node-count ${deleteCount}")
      val ret = proc.run()
      retVal = ret.exitValue
      if (retVal == 0) {
        logging.info(this, s"delete nodes success, ${deleteCount} nodes deleted")
      } else {
        exceptionMsg = "deleting node scripts failed: see the log"
        logging.error(this, s"deleting ${deleteCount} nodes failed: ${ret.exitValue}")
      }
    } catch {
      case e: Exception =>
        retVal = -1
        exceptionMsg = e.getMessage
        logging.error(this, s"deleting ${deleteCount} nodes failed with exception: ${e.getMessage}")
    }
    nodeHandling = false

    (retVal, exceptionMsg)
  }
}
