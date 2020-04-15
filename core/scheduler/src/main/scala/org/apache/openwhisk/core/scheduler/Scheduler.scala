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

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigValueFactory
import kamon.Kamon
import org.apache.openwhisk.common.Https.HttpsConfig
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector.{
  MessageProducer,
  MessagingProvider,
  Metric,
  ResourceMessage,
  SchedulerMessage
}
import org.apache.openwhisk.core.entity.SchedulerInstanceId
import org.apache.openwhisk.http.{BasicHttpService, BasicRasService}
import org.apache.openwhisk.spi.{Spi, SpiLoader}
import org.apache.openwhisk.utils.ExecutionContextFactory
import pureconfig.loadConfigOrThrow
import java.nio.charset.StandardCharsets

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object Scheduler {

  protected val protocol = loadConfigOrThrow[String]("whisk.scheduler.protocol")

  /**
   * An object which records the environment variables required for this component to run.
   */
  def requiredProperties =
    Map(servicePort -> 8080.toString) ++
      kafkaHosts

  def initKamon(instance: SchedulerInstanceId): Unit = {
    // Replace the hostname of the scheduler to the assigned id of the scheduler.
    val newKamonConfig = Kamon.config
      .withValue("kamon.environment.host", ConfigValueFactory.fromAnyRef(s"scheduler${instance.asString}"))
    Kamon.reconfigure(newKamonConfig)
  }

  def main(args: Array[String]): Unit = {
    try {
      ConfigMXBean.register()
    } catch {
      case _: Exception => None // might be registered by other process.
    }

    implicit val ec = ExecutionContextFactory.makeCachedThreadPoolExecutionContext()
    implicit val actorSystem: ActorSystem =
      ActorSystem(name = "scheduler-actor-system", defaultExecutionContext = Some(ec))
    implicit val logger = new AkkaLogging(akka.event.Logging.getLogger(actorSystem, this))

    start(args)
  }

  def start(args: Array[String])(implicit ec: ExecutionContext, actorSystem: ActorSystem, logger: Logging): Unit = {
    Kamon.loadReportersFromConfig()

    // Prepare Kamon shutdown
    CoordinatedShutdown(actorSystem).addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "shutdownKamon") { () =>
      logger.info(this, s"Shutting down Kamon with coordinated shutdown")
      Kamon.stopAllReporters().map(_ => Done)
    }

    // load values for the required properties from the environment
    implicit val config = new WhiskConfig(requiredProperties)

    def abort(message: String) = {
      logger.error(this, message)(TransactionId.scheduler)
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 30.seconds)
      sys.exit(1)
    }

    if (!config.isValid) {
      abort("Bad configuration, cannot start.")
    }

    require(args.length >= 1, "scheduler instance required")
    val schedulerInstance = SchedulerInstanceId(args(0))
    initKamon(schedulerInstance)
    logger.info(this, s"starting scheduler, the kafka host is: ${config.kafkaHosts}")

    // message provider
    val msgProvider = SpiLoader.get[MessagingProvider]
    Seq(
      (SchedulerMessage.topicName, SchedulerMessage.topicName, None),
      (ResourceMessage.topicName, ResourceMessage.topicName, None))
      .foreach {
        case (topic, topicConfigurationKey, maxMessageBytes) =>
          if (msgProvider.ensureTopic(config, topic, topicConfigurationKey, maxMessageBytes).isFailure) {
            abort(s"failure during msgProvider.ensureTopic for topic $topic")
          }
      }

    val producer = msgProvider.getProducer(config)
    val scheduler = try {
      SpiLoader.get[SchedulerProvider].instance(config, schedulerInstance, producer)
    } catch {
      case e: Exception => abort(s"Failed to initialize reactive scheduler: ${e.getMessage}")
    }

    val port = config.servicePort.toInt
    val httpsConfig =
      if (Scheduler.protocol == "https") Some(loadConfigOrThrow[HttpsConfig]("whisk.scheduler.https")) else None

    logger.info(this, s"starting http server, listening port: ${port}")
    val schedulerServer = SpiLoader.get[SchedulerServerProvider].instance(scheduler)
    BasicHttpService.startHttpService(schedulerServer.route, port, httpsConfig)(
      actorSystem,
      ActorMaterializer.create(actorSystem))
  }
}

/**
 * An Spi for providing scheduler implementation.
 */
trait SchedulerProvider extends Spi {
  def instance(config: WhiskConfig, instance: SchedulerInstanceId, producer: MessageProducer)(
    implicit actorSystem: ActorSystem,
    logging: Logging): SchedulerCore
}

// this trait can be used to add common implementation
trait SchedulerCore {
  def processResourceMessage(bytes: Array[Byte]): Future[Unit]
}

/**
 * An Spi for providing RestAPI implementation for scheduler.
 * The given scheduler may require corresponding RestAPI implementation.
 */
trait SchedulerServerProvider extends Spi {
  def instance(
    scheduler: SchedulerCore)(implicit ec: ExecutionContext, actorSystem: ActorSystem, logger: Logging): BasicRasService
}

class SchedulerTestService(scheduler: SchedulerCore)(implicit val actorSystem: ActorSystem,
                                                     implicit val logging: Logging)
    extends BasicRasService {
  override def routes(implicit transid: TransactionId): Route = {
    super.routes ~ testRoutes
  }

  private val testRoutes = {
    implicit val executionContext = actorSystem.dispatcher
    (pathPrefix("test") & post) {
      path("buy") {
        complete {
          scheduler
            .processResourceMessage(
              Metric(ResourceMessage.metricName.slotsNotEnough, 256).serialize.getBytes(StandardCharsets.UTF_8))
            .map(_ => JsObject("status" -> 0.toJson))
            .recover {
              case e: Exception =>
                JsObject("exception" -> e.getMessage.toJson)
            }
        }
      } ~ path("delete") {
        complete {
          scheduler
            .processResourceMessage(
              Metric(ResourceMessage.metricName.slotsTooMuch, 1).serialize.getBytes(StandardCharsets.UTF_8))
            .map(_ => JsObject("status" -> 0.toJson))
            .recover {
              case e: Exception =>
                JsObject("exception" -> e.getMessage.toJson)
            }
        }
      }
    }
  }
}

object DefaultSchedulerServer extends SchedulerServerProvider {
  override def instance(scheduler: SchedulerCore)(implicit ec: ExecutionContext,
                                                  actorSystem: ActorSystem,
                                                  logger: Logging): BasicRasService =
    new SchedulerTestService(scheduler)
}
