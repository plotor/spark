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

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


object ExampleClient {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("ExampleClient")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    val sparkEnv = sparkContext.env

    val rpcEnv = RpcEnv.create(
      "ExampleClient",
      "localhost",
      8080,
      conf,
      sparkEnv.securityManager,
      clientMode = true
    )
    val endpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 8080), "ExampleEndpoint")
    endpointRef.send(SayHello("hello, spark"))

    val future = endpointRef.ask[String](SayByeBye("byebye, spark"))
    future.onComplete {
      // scalastyle:off println
      case Success(msg) => println(s"success response, and msg is $msg")
      case Failure(msg) => println(s"failure response, and msg is $msg")
    }

    // scalastyle:off awaitresult
    Await.result(future, Duration("10s"))
    // scalastyle:on awaitresult

    println(endpointRef.askSync[String](SayHello("spark, see you again")))

    println(endpointRef.askSync[String]("bad message"))
    spark.stop()
    // scalastyle:on println
  }

}
