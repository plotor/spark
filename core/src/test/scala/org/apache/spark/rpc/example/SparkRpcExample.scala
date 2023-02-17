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

import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcEnv, RpcEnvConfig}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory

/**
 * Hello world server.
 *
 * @author zhenchao.wang 2023-01-12 20:46
 * @version 1.0.0
 */
// scalastyle:off println
object SparkRpcExample {

  private var rpcEnv: RpcEnv = _
  private val endpointName = "example_endpoint"

  def startServer(sparkConf: SparkConf): Unit = {
    val config = RpcEnvConfig(
      sparkConf, "example_server", "127.0.0.1", "127.0.0.1",
      8080, new SecurityManager(sparkConf), 0, clientMode = false)
    rpcEnv = new NettyRpcEnvFactory().create(config)
    rpcEnv.setupEndpoint(endpointName, new ExampleEndpoint(rpcEnv))
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf(true)
    startServer(sparkConf)

    val clientEnv = new NettyRpcEnvFactory().create(
      RpcEnvConfig(
        sparkConf, "example_client", "127.0.0.1", "127.0.0.1",
        0, new SecurityManager(sparkConf), 0, clientMode = true))
    val ref = clientEnv.setupEndpointRef(rpcEnv.address, endpointName)
    val resp = ref.askSync[String](Hello("zhenchao"))
    println(s"resp is '$resp'")

    clientEnv.shutdown()
    clientEnv.awaitTermination()

    rpcEnv.shutdown()
  }

}
