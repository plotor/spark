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

package org.apache.spark.sql.execution.benchmark

import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.math.NumberUtils

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object GlutenBenchmark extends SqlBasedBenchmark {

  /** Subclass can override this function to build their own SparkSession */
  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      // .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      // .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, 1)
      .config(UI_ENABLED.key, false)
      // .config(MAX_RESULT_SIZE.key, "8g")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    // scalastyle:off println
    println(s"""args: ${mainArgs.mkString("[", ",", "]")}""")
    // scalastyle:on println

    val numIters = NumberUtils.toInt(mainArgs(0), 10)
    val glutenMode = BooleanUtils.toBoolean(mainArgs(1))
    val scene = mainArgs(2)

    if ("hash_join".equalsIgnoreCase(scene)) {
      runHashJoinBenchmark(glutenMode, numIters)
    }

    if ("aggregations".equalsIgnoreCase(scene)) {
      runBenchmark("Aggregations with Text") {

        val sql = "SELECT id, count(name) FROM tit GROUP BY id"

        val benchmark = new Benchmark("Gluten Benchmark", 78635543L, output = output)

        benchmark.addCase("Aggregations with Text", numIters) { _ =>
          withSQLConf() {
            spark.sql(sql).noop()
          }
        }

        benchmark.run()
      }

      runBenchmark("Aggregations with Parquet") {

        val sql = "SELECT id, count(name) FROM t_user GROUP BY id"

        val benchmark = new Benchmark("Gluten Benchmark", 69274021L, output = output)

        benchmark.addCase("Aggregations with Parquet", numIters) { _ =>
          withSQLConf() {
            spark.sql(sql).noop()
          }
        }

        benchmark.run()
      }
    }

  }

  def runHashJoinBenchmark(glutenMode: Boolean, numIters: Int): Unit = {
    // runBenchmark("Hash Join with Text") {
    //
    //  val sql = "SELECT count(*) FROM ti1, ti2 WHERE ti1.id1 = ti2.id1"
    //
    //  val benchmark = new Benchmark("Gluten Benchmark", 134217728L, output = output)
    //
    //  if (!glutenMode) {
    //    benchmark.addCase("Hash Join with Text (preferSortMergeJoin=true)", numIters) { _ =>
    //      withSQLConf(
    //        SQLConf.PREFER_SORTMERGEJOIN.key -> "true"
    //      ) {
    //        spark.sql(sql).noop()
    //      }
    //    }
    //  }
    //
    //  benchmark.addCase("Hash Join with Text (preferSortMergeJoin=false)", numIters) { _ =>
    //    withSQLConf(
    //      SQLConf.PREFER_SORTMERGEJOIN.key -> "false"
    //    ) {
    //      spark.sql(sql).noop()
    //    }
    //  }
    //
    //  benchmark.run()
    // }

    runBenchmark("Hash Join with Parquet") {

      val sql = "SELECT count(*) FROM tpi1, tpi2 WHERE tpi1.id = tpi2.id"

      val benchmark = new Benchmark("Gluten Benchmark", 268435456L, output = output)

      if (!glutenMode) {
        benchmark.addCase("Hash Join with Parquet (preferSortMergeJoin=true)", numIters) { _ =>
          withSQLConf(
            SQLConf.PREFER_SORTMERGEJOIN.key -> "true"
          ) {
            spark.sql(sql).noop()
          }
        }
      }

      benchmark.addCase("Hash Join with Parquet (preferSortMergeJoin=false)", numIters) { _ =>
        withSQLConf(
          SQLConf.PREFER_SORTMERGEJOIN.key -> "false"
        ) {
          spark.sql(sql).noop()
        }
      }

      benchmark.run()
    }
  }

}
