/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rpc.example

import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
 * Example endpoint.
 *
 * @author zhenchao.wang 2023-01-12 20:11
 * @version 1.0.0
 */
class ExampleEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  override def onStart(): Unit = {
    // scalastyle:off println
    println("start example endpoint")
  }

  /**
   * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Hello(msg) =>
      println(s"receive hello message: $msg")
      context.reply(s"hello, $msg")
    case ByeBye(msg) =>
      println(s"receive bye-bye message: $msg")
      context.reply(s"bye-bye, $msg")
    case unknownMsg =>
      context.reply(s"unknown message $unknownMsg from ${context.senderAddress}")
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   */
  override def onStop(): Unit = {
    // scalastyle:off println
    println("stop example endpoint")
  }

}
