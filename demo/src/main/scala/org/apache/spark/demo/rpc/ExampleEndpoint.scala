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

package org.apache.spark.demo.rpc

import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

class ExampleEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = println(s"Start example endpoint, and addr is ${rpcEnv.address}")

  override def receive: PartialFunction[Any, Unit] = {
    case SayHello(msg) => println(s"receive hello msg: $msg")
    case SayByeBye(msg) => println(s"receive byebye msg: $msg")
    case _ => println(s"receive unknown msg type")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHello(msg) =>
      println(s"receive hello msg: $msg")
      context.reply(s"Hello, the msg received is '$msg'")
    case SayByeBye(msg) =>
      println(s"receive byebye msg: $msg")
      context.reply(s"Byebye, the msg received is '$msg'")
    case _ =>
      println(s"receive unknown msg type")
      context.sendFailure(new IllegalStateException("Unknown msg type"))
  }

  override def onStop(): Unit = println("Stop example endpoint")
}

case class SayHello(msg: String)

case class SayByeBye(msg: String)
