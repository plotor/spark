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

package org.apache.spark.storage

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{DeterministicLevel, RDD, RDDOperationScope}
import org.apache.spark.util.Utils

/**
 * 描述 RDD 的元信息
 */
@DeveloperApi
class RDDInfo(
    val id: Int,
    var name: String,
    val numPartitions: Int, // 分区数量
    var storageLevel: StorageLevel,
    val isBarrier: Boolean,
    val parentIds: Seq[Int], // 前置依赖的 RDD 的 ID 集合
    val callSite: String = "", // 对应用户调用栈信息
    val scope: Option[RDDOperationScope] = None,
    val outputDeterministicLevel: DeterministicLevel.Value = DeterministicLevel.DETERMINATE)
  extends Ordered[RDDInfo] {

  // 缓存的分区数量
  var numCachedPartitions = 0
  // 使用的内存大小
  var memSize = 0L
  // 使用的磁盘大小
  var diskSize = 0L

  def isCached: Boolean = (memSize + diskSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(diskSize))
  }

  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

private[spark] object RDDInfo {
  /**
   * 构造 RDD 对应的元信息
   */
  def fromRdd(rdd: RDD[_]): RDDInfo = {
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    val parentIds = rdd.dependencies.map(_.rdd.id)
    val callsiteLongForm = Option(SparkEnv.get)
      .map(_.conf.get(EVENT_LOG_CALLSITE_LONG_FORM))
      .getOrElse(false)

    val callSite = if (callsiteLongForm) {
      rdd.creationSite.longForm
    } else {
      rdd.creationSite.shortForm
    }
    new RDDInfo(rdd.id, rddName, rdd.partitions.length,
      rdd.getStorageLevel, rdd.isBarrier(), parentIds, callSite, rdd.scope,
      rdd.outputDeterministicLevel)
  }
}
