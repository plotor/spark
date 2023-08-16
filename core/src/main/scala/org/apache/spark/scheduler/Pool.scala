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

package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String,
    // 调度模式
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  val weight = initWeight
  val minShare = initMinShare
  // 正在运行的任务数
  var runningTasks = 0
  // 调度优先级
  val priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  val name = poolName
  // 父 Pool
  var parent: Pool = null

  // 获取调度模式对应的算法实例
  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }

  override def isSchedulable: Boolean = true

  override def addSchedulable(schedulable: Schedulable): Unit = {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable): Unit = {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  /**
   * 如果某个 Executor 异常，则需要将其上的 Task 标识已完成，并重新提交执行
   */
  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  override def executorDecommission(executorId: String): Unit = {
    schedulableQueue.asScala.foreach(_.executorDecommission(executorId))
  }

  override def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue.filter(_.isSchedulable)
    }
    sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int): Unit = {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int): Unit = {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
