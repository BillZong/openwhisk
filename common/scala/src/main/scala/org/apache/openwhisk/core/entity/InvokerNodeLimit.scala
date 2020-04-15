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

package org.apache.openwhisk.core.entity

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import spray.json._
import org.apache.openwhisk.core.ConfigKeys
import pureconfig.loadConfigOrThrow

case class InvokerNodeLimitConfig(min: Int, max: Int)

/**
 * InvokerNodeLimit encapsulates allowed node count for a cluster. The limit must be within a
 * permissible range (by default [1, 10]).
 *
 * It is a value type (hence == is .equals, immutable and cannot be assigned null).
 * The constructor is private so that argument requirements are checked and normalized
 * before creating a new instance.
 *
 * @nodes node count of invokers.
 */
protected[entity] class InvokerNodeLimit private (val nodes: Int) extends AnyVal

protected[core] object InvokerNodeLimit extends ArgNormalizer[InvokerNodeLimit] {
  val config = loadConfigOrThrow[InvokerNodeLimitConfig](ConfigKeys.schedulerInvokerNodeLimits)

  /** These values are set once at the beginning. Dynamic configuration updates are not supported at the moment. */
  protected[core] val MIN_NODES: Int = config.min
  protected[core] val MAX_NODES: Int = config.max

  /** Gets InvokerNodeLimit with default (lowest) value */
  protected[core] def apply(): InvokerNodeLimit = InvokerNodeLimit(MIN_NODES)

  @throws[IllegalArgumentException]
  protected[core] def apply(nodes: Int): InvokerNodeLimit = {
    require(nodes >= MIN_NODES, s"nodes ${nodes} below allowed threshold of $MIN_NODES")
    require(nodes <= MAX_NODES, s"nodes ${nodes} below allowed threshold of $MAX_NODES")
    new InvokerNodeLimit(nodes)
  }

  override protected[core] implicit val serdes = new RootJsonFormat[InvokerNodeLimit] {
    def write(n: InvokerNodeLimit) = JsNumber(n.nodes)

    def read(value: JsValue) =
      Try {
        val JsNumber(nodes) = value
        require(nodes.isWhole, "memory limit must be whole number")
        InvokerNodeLimit(nodes.toInt)
      } match {
        case Success(limit)                       => limit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable)                => deserializationError("invoker nodes limit malformed", e)
      }
  }
}
