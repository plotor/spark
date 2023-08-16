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

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import scala.collection.immutable.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.math.max
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.util.{AccumulatorV2, Clock, LongAccumulator, SystemClock, Utils}
import org.apache.spark.util.collection.MedianHeap

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and handleSuccessfulTask/handleFailedTask, which tells it that one of its tasks changed state
 *  (e.g. finished/failed).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails this number of times, the entire
 *                        task set will be aborted
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet, // 管理的 Task 集合
    val maxTaskFailures: Int, // 最大任务失败次数
    healthTracker: Option[HealthTracker] = None,
    clock: Clock = new SystemClock()) extends Schedulable with Logging {

  private val conf = sched.sc.conf

  // SPARK-21563 make a copy of the jars/files so they are consistent across the TaskSet
  private val addedJars = HashMap[String, Long](sched.sc.addedJars.toSeq: _*)
  private val addedFiles = HashMap[String, Long](sched.sc.addedFiles.toSeq: _*)
  private val addedArchives = HashMap[String, Long](sched.sc.addedArchives.toSeq: _*)

  val maxResultSize = conf.get(config.MAX_RESULT_SIZE)

  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  private val isShuffleMapTasks = tasks(0).isInstanceOf[ShuffleMapTask]
  private[scheduler] val partitionToIndex = tasks.zipWithIndex
    .map { case (t, idx) => t.partitionId -> idx }.toMap
  val numTasks = tasks.length
  // 记录对应 Task 的运行次数
  val copiesRunning = new Array[Int](numTasks)

  // 是否开启推断执行
  val speculationEnabled = conf.get(SPECULATION_ENABLED)
  // Quantile of tasks at which to start speculation
  // 推断执行分位数，即当一个 Stage 中已完成的 Task 数目超过该分位数时才启动推断执行
  val speculationQuantile = conf.get(SPECULATION_QUANTILE)
  // 运行时长比已完成 Task 时长中位数超过该比例的 Task 才会被推断执行
  val speculationMultiplier = conf.get(SPECULATION_MULTIPLIER)
  val minFinishedForSpeculation = math.max((speculationQuantile * numTasks).floor.toInt, 1)
  // User provided threshold for speculation regardless of whether the quantile has been reached
  val speculationTaskDurationThresOpt = conf.get(SPECULATION_TASK_DURATION_THRESHOLD)
  // SPARK-29976: Only when the total number of tasks in the stage is less than or equal to the
  // number of slots on a single executor, would the task manager speculative run the tasks if
  // their duration is longer than the given threshold. In this way, we wouldn't speculate too
  // aggressively but still handle basic cases.
  // SPARK-30417: #cores per executor might not be set in spark conf for standalone mode, then
  // the value of the conf would 1 by default. However, the executor would use all the cores on
  // the worker. Therefore, CPUS_PER_TASK is okay to be greater than 1 without setting #cores.
  // To handle this case, we set slots to 1 when we don't know the executor cores.
  // TODO: use the actual number of slots for standalone mode.
  val speculationTasksLessEqToSlots = {
    val rpId = taskSet.resourceProfileId
    val resourceProfile = sched.sc.resourceProfileManager.resourceProfileFromId(rpId)
    val slots = if (!resourceProfile.isCoresLimitKnown) {
      1
    } else {
      resourceProfile.maxTasksPerExecutor(conf)
    }
    numTasks <= slots
  }

  private val executorDecommissionKillInterval =
    conf.get(EXECUTOR_DECOMMISSION_KILL_INTERVAL).map(TimeUnit.SECONDS.toMillis)

  // For each task, tracks whether a copy of the task has succeeded. A task will also be
  // marked as "succeeded" if it failed with a fetch failure, in which case it should not
  // be re-run because the missing map data needs to be regenerated first.
  // 标识对应 Task 是否成功
  val successful = new Array[Boolean](numTasks)
  // 标识对应 Task 执行失败次数
  private val numFailures = new Array[Int](numTasks)

  // Add the tid of task into this HashSet when the task is killed by other attempt tasks.
  // This happened while we set the `spark.speculation` to true. The task killed by others
  // should not resubmit while executor lost.
  private val killedByOtherAttempt = new HashSet[Long]

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  // 执行成功的 Task 数量
  private[scheduler] var tasksSuccessful = 0

  val weight = 1
  val minShare = 0
  var priority = taskSet.priority
  val stageId = taskSet.stageId
  val name = "TaskSet_" + taskSet.id
  var parent: Pool = null
  private var totalResultSize = 0L
  // 计算过的 Task 数量
  private var calculatedTasks = 0

  private[scheduler] val taskSetExcludelistHelperOpt: Option[TaskSetExcludelist] = {
    healthTracker.map { _ =>
      new TaskSetExcludelist(sched.sc.listenerBus, conf, stageId, taskSet.stageAttemptId, clock)
    }
  }

  // 正在运行的 Task ID 集合
  private[scheduler] val runningTasksSet = new HashSet[Long]

  override def runningTasks: Int = runningTasksSet.size

  def someAttemptSucceeded(tid: Long): Boolean = {
    successful(taskInfos(tid).index)
  }

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  private[scheduler] var isZombie = false // 标识是否处于僵尸状态

  // Whether the taskSet run tasks from a barrier stage. Spark must launch all the tasks at the
  // same time for a barrier stage.
  private[scheduler] def isBarrier = taskSet.tasks.nonEmpty && taskSet.tasks(0).isBarrier

  // Barrier tasks that are pending to launch in a single resourceOffers round. Tasks will only get
  // launched when all tasks are added to this pending list in a single round. Otherwise, we'll
  // revert everything we did during task scheduling.
  private[scheduler] val barrierPendingLaunchTasks = new HashMap[Int, BarrierPendingLaunchTask]()

  // Record the last log time of the barrier TaskSetManager that failed to get all tasks launched.
  private[scheduler] var lastResourceOfferFailLogTime = clock.getTimeMillis()

  // Store tasks waiting to be scheduled by locality preferences
  // 等待执行的 Task 集合
  private[scheduler] val pendingTasks = new PendingTasksByLocality()

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet. The HashSet here ensures that we do not add
  // duplicate speculatable tasks.
  // 可以被推断执行的 Task ID 列表
  private[scheduler] val speculatableTasks = new HashSet[Int]

  // Store speculatable tasks by locality preferences
  private[scheduler] val pendingSpeculatableTasks = new PendingTasksByLocality()

  // Task index, start and finish time for each task attempt (indexed by task ID)
  // TaskId -> TaskInfo
  private[scheduler] val taskInfos = new HashMap[Long, TaskInfo]

  // Use a MedianHeap to record durations of successful tasks so we know when to launch
  // speculative tasks. This is only used when speculation is enabled, to avoid the overhead
  // of inserting into the heap when the heap won't be used.
  val successfulTaskDurations = new MedianHeap()

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  // 缓存异常信息、发生的次数以及对应时间戳
  private val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  addPendingTasks()

  private def addPendingTasks(): Unit = {
    val (_, duration) = Utils.timeTakenMs {
      for (i <- (0 until numTasks).reverse) {
        // 将 Task 按照位置偏好添加到对应的 pending 集合中
        addPendingTask(i, resolveRacks = false)
      }
      // Resolve the rack for each host. This can be slow, so de-dupe the list of hosts,
      // and assign the rack to all relevant task indices.
      val (hosts, indicesForHosts) = pendingTasks.forHost.toSeq.unzip
      val racks = sched.getRacksForHosts(hosts)
      racks.zip(indicesForHosts).foreach {
        case (Some(rack), indices) =>
          pendingTasks.forRack.getOrElseUpdate(rack, new ArrayBuffer) ++= indices
        case (None, _) => // no rack, nothing to do
      }
    }
    logDebug(s"Adding pending tasks took $duration ms")
  }

  /**
   * Track the set of locality levels which are valid given the tasks locality preferences and
   * the set of currently available executors.  This is updated as executors are added and removed.
   * This allows a performance optimization, of skipping levels that aren't relevant (e.g., skip
   * PROCESS_LOCAL if no tasks could be run PROCESS_LOCAL for the current set of executors).
   *
   * 记录本地可用且有效的 TaskLocality 级别
   */
  private[scheduler] var myLocalityLevels = computeValidLocalityLevels()

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last reset the locality wait timer, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task when resetting the timer
  private val legacyLocalityWaitReset = conf.get(LEGACY_LOCALITY_WAIT_RESET)
  private var currentLocalityIndex = 0 // Index of our current locality level in validLocalityLevels
  private var lastLocalityWaitResetTime = clock.getTimeMillis()  // Time we last reset locality wait

  // Time to wait at each level
  // 对应每个本地级别的任务等待执行时间
  private[scheduler] var localityWaits = myLocalityLevels.map(getLocalityWait)

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  private[scheduler] var emittedTaskSizeWarning = false

  /**
   * Add a task to all the pending-task lists that it should be on.
   *
   * 将 Task 按照位置偏好添加到对应的 pending 集合中
   */
  private[spark] def addPendingTask(
      index: Int,
      resolveRacks: Boolean = true,
      speculatable: Boolean = false): Unit = {
    // A zombie TaskSetManager may reach here while handling failed task.
    if (isZombie) return
    val pendingTaskSetToAddTo = if (speculatable) pendingSpeculatableTasks else pendingTasks
    for (loc <- tasks(index).preferredLocations) {
      loc match {
        case e: ExecutorCacheTaskLocation =>
          pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
        case e: HDFSCacheTaskLocation =>
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
              ", but there are no executors alive there.")
          }
        case _ =>
      }
      pendingTaskSetToAddTo.forHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index

      if (resolveRacks) {
        sched.getRackForHost(loc.host).foreach { rack =>
          pendingTaskSetToAddTo.forRack.getOrElseUpdate(rack, new ArrayBuffer) += index
        }
      }
    }

    if (tasks(index).preferredLocations == Nil) {
      pendingTaskSetToAddTo.noPrefs += index
    }

    pendingTaskSetToAddTo.all += index
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   *
   * 从给定的 Task 列表中按照索引从后往前寻找并返回满足条件的 Task 索引
   */
  private def dequeueTaskFromList(
      execId: String,
      host: String,
      list: ArrayBuffer[Int],
      speculative: Boolean = false): Option[Int] = {
    var indexOffset = list.size
    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset)
      if (!isTaskExcludededOnExecOrNode(index, execId, host) &&
          !(speculative && hasAttemptOnHost(index, host))) {
        // This should almost always be list.trimEnd(1) to remove tail
        list.remove(indexOffset)
        // Speculatable task should only be launched when at most one copy of the
        // original task is running
        if (!successful(index)) {
          if (copiesRunning(index) == 0 && !barrierPendingLaunchTasks.contains(index)) {
            return Some(index)
          } else if (speculative && copiesRunning(index) == 1) {
            return Some(index)
          }
        }
      }
    }
    None
  }

  /** Check whether a task once ran an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  private def isTaskExcludededOnExecOrNode(index: Int, execId: String, host: String): Boolean = {
    taskSetExcludelistHelperOpt.exists { excludeList =>
      excludeList.isNodeExcludedForTask(host, index) ||
        excludeList.isExecutorExcludedForTask(execId, index)
    }
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   *
   * 依据指定的 Executor、Host，以及本地偏好找出满足条件的 1 个 Task
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def dequeueTask(
      execId: String,
      host: String,
      maxLocality: TaskLocality.Value): Option[(Int, TaskLocality.Value, Boolean)] = {
    // Tries to schedule a regular task first; if it returns None, then schedules
    // a speculative task
    dequeueTaskHelper(execId, host, maxLocality, false).orElse(dequeueTaskHelper(execId, host, maxLocality, true))
  }

  protected def dequeueTaskHelper(
      execId: String,
      host: String,
      maxLocality: TaskLocality.Value,
      speculative: Boolean): Option[(Int, TaskLocality.Value, Boolean)] = {
    if (speculative && speculatableTasks.isEmpty) {
      return None
    }
    val pendingTaskSetToUse = if (speculative) pendingSpeculatableTasks else pendingTasks
    def dequeue(list: ArrayBuffer[Int]): Option[Int] = {
      val task = dequeueTaskFromList(execId, host, list, speculative)
      if (speculative && task.isDefined) {
        speculatableTasks -= task.get
      }
      task
    }

    dequeue(pendingTaskSetToUse.forExecutor.getOrElse(execId, ArrayBuffer())).foreach { index =>
      return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      dequeue(pendingTaskSetToUse.forHost.getOrElse(host, ArrayBuffer())).foreach { index =>
        return Some((index, TaskLocality.NODE_LOCAL, speculative))
      }
    }

    // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      dequeue(pendingTaskSetToUse.noPrefs).foreach { index =>
        return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeue(pendingTaskSetToUse.forRack.getOrElse(rack, ArrayBuffer()))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, speculative))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      dequeue(pendingTaskSetToUse.all).foreach { index =>
        return Some((index, TaskLocality.ANY, speculative))
      }
    }
    None
  }

  private[scheduler] def resetDelayScheduleTimer(
      minLocality: Option[TaskLocality.TaskLocality]): Unit = {
    lastLocalityWaitResetTime = clock.getTimeMillis()
    for (locality <- minLocality) {
      currentLocalityIndex = getLocalityIndex(locality)
    }
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * @param execId the executor Id of the offered resource
   * @param host  the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
   * @param taskCpus the number of CPUs for the task
   * @param taskResourceAssignments the resource assignments for the task
   *
   * @return Triple containing:
   *         (TaskDescription of launched task if any,
   *         rejected resource due to delay scheduling?,
   *         dequeued task index)
   */
  @throws[TaskNotSerializableException]
  def resourceOffer( // 依据本地偏好为给定 Task 分配资源
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality,
      taskCpus: Int = sched.CPUS_PER_TASK,
      taskResourceAssignments: Map[String, ResourceInformation] = Map.empty)
    : (Option[TaskDescription], Boolean, Int) =
  {
    val offerExcluded = taskSetExcludelistHelperOpt.exists { excludeList =>
      excludeList.isNodeExcludedForTaskSet(host) ||
        excludeList.isExecutorExcludedForTaskSet(execId)
    }
    // 不是僵尸状态 || 分配的 host 或 Executor 不在黑名单
    if (!isZombie && !offerExcluded) {
      val curTime = clock.getTimeMillis()

      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {
        // 计算获取最优的本地级别
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      var dequeuedTaskIndex: Option[Int] = None
      val taskDescription = {
        // 依据指定的 Executor、Host，以及本地偏好找出满足条件的 1 个 Task
        dequeueTask(execId, host, allowedLocality)
          .map { case (index, taskLocality, speculative) =>
            dequeuedTaskIndex = Some(index)
            if (legacyLocalityWaitReset && maxLocality != TaskLocality.NO_PREF) {
              resetDelayScheduleTimer(Some(taskLocality))
            }
            // Barrier Stage
            if (isBarrier) {
              barrierPendingLaunchTasks(index) =
                BarrierPendingLaunchTask(
                  execId,
                  host,
                  index,
                  taskLocality,
                  taskResourceAssignments)
              // return null since the TaskDescription for the barrier task is not ready yet
              null
            } else {
              // 异步提交 Task 执行，并返回对应的 TaskDescription
              prepareLaunchingTask(
                execId,
                host,
                index,
                taskLocality,
                speculative,
                taskCpus,
                taskResourceAssignments,
                curTime)
            }
          }
      }

      val hasPendingTasks = pendingTasks.all.nonEmpty || pendingSpeculatableTasks.all.nonEmpty
      val hasScheduleDelayReject =
        taskDescription.isEmpty &&
          maxLocality == TaskLocality.ANY &&
          hasPendingTasks
      (taskDescription, hasScheduleDelayReject, dequeuedTaskIndex.getOrElse(-1))
    } else {
      (None, false, -1)
    }
  }

  def prepareLaunchingTask(
      execId: String,
      host: String,
      index: Int,
      taskLocality: TaskLocality.Value,
      speculative: Boolean,
      taskCpus: Int,
      taskResourceAssignments: Map[String, ResourceInformation],
      launchTime: Long): TaskDescription = {
    // Found a task; do some bookkeeping and return a task description
    // 依据 index 获取对应的 Task
    val task = tasks(index)
    // 生成 Task ID
    val taskId = sched.newTaskId()
    // Do various bookkeeping
    copiesRunning(index) += 1
    val attemptNum = taskAttempts(index).size
    // 构建 Task 元数据信息
    val info = new TaskInfo(
      taskId, index, attemptNum, task.partitionId, launchTime,
      execId, host, taskLocality, speculative)
    taskInfos(taskId) = info
    taskAttempts(index) = info :: taskAttempts(index)
    // Serialize and return the task
    val serializedTask: ByteBuffer = try {
      // 对 Task 执行序列化
      ser.serialize(task)
    } catch {
      // If the task cannot be serialized, then there's no point to re-attempt the task,
      // as it will always fail. So just abort the whole task-set.
      case NonFatal(e) =>
        val msg = s"Failed to serialize task $taskId, not attempting to retry it."
        logError(msg, e)
        abort(s"$msg Exception during serialization: $e")
        throw SparkCoreErrors.failToSerializeTaskError(e)
    }
    if (serializedTask.limit() > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024 &&
      !emittedTaskSizeWarning) {
      emittedTaskSizeWarning = true
      logWarning(s"Stage ${task.stageId} contains a task of very large size " +
        s"(${serializedTask.limit() / 1024} KiB). The maximum recommended task size is " +
        s"${TaskSetManager.TASK_SIZE_TO_WARN_KIB} KiB.")
    }
    // 记录 TaskId 到 runningTasksSet
    addRunningTask(taskId)

    // We used to log the time it takes to serialize the task, but task size is already
    // a good proxy to task serialization time.
    // val timeTaken = clock.getTime() - startTime
    val tName = taskName(taskId)
    logInfo(s"Starting $tName ($host, executor ${info.executorId}, " +
      s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit()} bytes) " +
      s"taskResourceAssignments ${taskResourceAssignments}")

    // 发送 BeginEvent 事件通知启动 Task 执行
    sched.dagScheduler.taskStarted(task, info)
    new TaskDescription(
      taskId,
      attemptNum,
      execId,
      tName,
      index,
      task.partitionId,
      addedFiles,
      addedJars,
      addedArchives,
      task.localProperties,
      taskCpus,
      taskResourceAssignments,
      serializedTask)
  }

  def taskName(tid: Long): String = {
    val info = taskInfos.get(tid)
    assert(info.isDefined, s"Can not find TaskInfo for task (TID $tid)")
    s"task ${info.get.id} in stage ${taskSet.id} (TID $tid)"
  }

  private def maybeFinishTaskSet(): Unit = {
    // 僵尸状态，且正在运行中的 Task 数目为 0，标识 TaskSet 已完成
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
      if (tasksSuccessful == numTasks) {
        healthTracker.foreach(_.updateExcludedForSuccessfulTaskSet(
          taskSet.stageId,
          taskSet.stageAttemptId,
          taskSetExcludelistHelperOpt.get.execToFailures))
      }
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   *
   * 获取最优有效级别
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    // 是否存在等待被调度的 Task
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        // Task 没有在运行，且 Task 没有成功
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    // 给定的 Task 集合是否存在等待被调度的 Task
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          // 存在需要等待被调度的任务
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasks.forExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasks.forHost)
        case TaskLocality.NO_PREF => pendingTasks.noPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasks.forRack)
      }
      // 不存在需要被调度的 Task
      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        lastLocalityWaitResetTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1
      } else if (curTime - lastLocalityWaitResetTime >= localityWaits(currentLocalityIndex)) {
        // Jump to the next locality level, and reset lastLocalityWaitResetTime so that the next
        // locality wait timer doesn't immediately expire
        lastLocalityWaitResetTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        currentLocalityIndex += 1
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    // 未能找到，返回最低有效级别
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = { // 获取 locality 对应在 myLocalityLevels 中的 index
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Check whether the given task set has been excluded to the point that it can't run anywhere.
   *
   * It is possible that this taskset has become impossible to schedule *anywhere* due to the
   * failures that lead executors being excluded from the ones we can run on. The most common
   * scenario would be if there are fewer executors than spark.task.maxFailures.
   * We need to detect this so we can avoid the job from being hung. We try to acquire new
   * executor/s by killing an existing idle excluded executor.
   *
   * There's a tradeoff here: we could make sure all tasks in the task set are schedulable, but that
   * would add extra time to each iteration of the scheduling loop. Here, we take the approach of
   * making sure at least one of the unscheduled tasks is schedulable. This means we may not detect
   * the hang as quickly as we could have, but we'll always detect the hang eventually, and the
   * method is faster in the typical case. In the worst case, this method can take
   * O(maxTaskFailures + numTasks) time, but it will be faster when there haven't been any task
   * failures (this is because the method picks one unscheduled task, and then iterates through each
   * executor until it finds one that the task isn't excluded on).
   */
  private[scheduler] def getCompletelyExcludedTaskIfAny(
      hostToExecutors: HashMap[String, HashSet[String]]): Option[Int] = {
    taskSetExcludelistHelperOpt.flatMap { taskSetExcludelist =>
      val appHealthTracker = healthTracker.get
      // Only look for unschedulable tasks when at least one executor has registered. Otherwise,
      // task sets will be (unnecessarily) aborted in cases when no executors have registered yet.
      if (hostToExecutors.nonEmpty) {
        // find any task that needs to be scheduled
        val pendingTask: Option[Int] = {
          // usually this will just take the last pending task, but because of the lazy removal
          // from each list, we may need to go deeper in the list.  We poll from the end because
          // failed tasks are put back at the end of allPendingTasks, so we're more likely to find
          // an unschedulable task this way.
          val indexOffset = pendingTasks.all.lastIndexWhere { indexInTaskSet =>
            copiesRunning(indexInTaskSet) == 0 && !successful(indexInTaskSet)
          }
          if (indexOffset == -1) {
            None
          } else {
            Some(pendingTasks.all(indexOffset))
          }
        }

        pendingTask.find { indexInTaskSet =>
          // try to find some executor this task can run on.  Its possible that some *other*
          // task isn't schedulable anywhere, but we will discover that in some later call,
          // when that unschedulable task is the last task remaining.
          hostToExecutors.forall { case (host, execsOnHost) =>
            // Check if the task can run on the node
            val nodeExcluded =
              appHealthTracker.isNodeExcluded(host) ||
                taskSetExcludelist.isNodeExcludedForTaskSet(host) ||
                taskSetExcludelist.isNodeExcludedForTask(host, indexInTaskSet)
            if (nodeExcluded) {
              true
            } else {
              // Check if the task can run on any of the executors
              execsOnHost.forall { exec =>
                appHealthTracker.isExecutorExcluded(exec) ||
                  taskSetExcludelist.isExecutorExcludedForTaskSet(exec) ||
                  taskSetExcludelist.isExecutorExcludedForTask(exec, indexInTaskSet)
              }
            }
          }
        }
      } else {
        None
      }
    }
  }

  private[scheduler] def abortSinceCompletelyExcludedOnFailure(indexInTaskSet: Int): Unit = {
    taskSetExcludelistHelperOpt.foreach { taskSetExcludelist =>
      val partition = tasks(indexInTaskSet).partitionId
      abort(s"""
         |Aborting $taskSet because task $indexInTaskSet (partition $partition)
         |cannot run anywhere due to node and executor excludeOnFailure.
         |Most recent failure:
         |${taskSetExcludelist.getLatestFailureReason}
         |
         |ExcludeOnFailure behavior can be configured via spark.excludeOnFailure.*.
         |""".stripMargin)
    }
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    val info = taskInfos(tid)
    info.markGettingResult(clock.getTimeMillis())
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes.
   * This check does not apply to shuffle map tasks as they return map status and metrics updates,
   * which will be discarded by the driver after being processed.
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
    totalResultSize += size
    calculatedTasks += 1
    if (!isShuffleMapTasks && maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than ${config.MAX_RESULT_SIZE.key} " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * Marks a task as successful and notifies the DAGScheduler that the task has ended.
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    val info = taskInfos(tid)
    // SPARK-37300: when the task was already finished state, just ignore it,
    // so that there won't cause successful and tasksSuccessful wrong result.
    if(info.finished) {
      return
    }
    val index = info.index
    // Check if any other attempt succeeded before this and this attempt has not been handled
    // 对应的 Task 已经成功，且被其他尝试给 Kill 掉了
    if (successful(index) && killedByOtherAttempt.contains(tid)) {
      // Undo the effect on calculatedTasks and totalResultSize made earlier when
      // checking if can fetch more results
      calculatedTasks -= 1
      val resultSizeAcc = result.accumUpdates.find(a =>
        a.name == Some(InternalAccumulator.RESULT_SIZE))
      if (resultSizeAcc.isDefined) {
        totalResultSize -= resultSizeAcc.get.asInstanceOf[LongAccumulator].value
      }

      // Handle this task as a killed task
      handleFailedTask(tid, TaskState.KILLED,
        TaskKilled("Finish but did not commit due to another attempt succeeded"))
      return
    }

    // 标识 Task 已完成
    info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    if (speculationEnabled) {
      // 更新已完成的任务执行时长中位数
      successfulTaskDurations.insert(info.duration)
    }
    // 移除 running 状态
    removeRunningTask(tid)

    // Kill any other attempts for the same task (since those are unnecessary now that one
    // attempt completed successfully).
    // Kill 由其它尝试启动的 Task
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for ${taskName(attemptInfo.taskId)}" +
        s" on ${attemptInfo.host} as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      killedByOtherAttempt += attemptInfo.taskId
      sched.backend.killTask(
        attemptInfo.taskId,
        attemptInfo.executorId,
        interruptThread = true,
        reason = "another attempt succeeded")
    }
    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo(s"Finished ${taskName(info.taskId)} in ${info.duration} ms " +
        s"on ${info.host} (executor ${info.executorId}) ($tasksSuccessful/$numTasks)")
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      numFailures(index) = 0
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo(s"Ignoring task-finished event for ${taskName(info.taskId)} " +
        s"because it has already completed successfully")
    }
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    // 发送 CompletionEvent 事件，通知 Task 已执行完成
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates,
      result.metricPeaks, info)
    maybeFinishTaskSet()
  }

  private[scheduler] def markPartitionCompleted(partitionId: Int): Unit = {
    partitionToIndex.get(partitionId).foreach { index =>
      if (!successful(index)) {
        tasksSuccessful += 1
        successful(index) = true
        numFailures(index) = 0
        if (tasksSuccessful == numTasks) {
          isZombie = true
        }
        maybeFinishTaskSet()
      }
    }
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason): Unit = {
    val info = taskInfos(tid)
    // SPARK-37300: when the task was already finished state, just ignore it,
    // so that there won't cause copiesRunning wrong result.
    if (info.finished) {
      return
    }
    // 从 Running TaskSet 中移除当前 Task
    removeRunningTask(tid)
    // 标识 Task 已完成
    info.markFinished(state, clock.getTimeMillis())
    val index = info.index
    copiesRunning(index) -= 1
    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
    var metricPeaks: Array[Long] = Array.empty
    val failureReason = s"Lost ${taskName(tid)} (${info.host} " +
      s"executor ${info.executorId}): ${reason.toErrorString}"
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        isZombie = true

        if (fetchFailed.bmAddress != null) {
          healthTracker.foreach(_.updateExcludedForFetchFailure(
            fetchFailed.bmAddress.host, fetchFailed.bmAddress.executorId))
        }

        None

      case ef: ExceptionFailure =>
        // ExceptionFailure's might have accumulator updates
        accumUpdates = ef.accums
        metricPeaks = ef.metricPeaks.toArray
        val task = taskName(tid)
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError(s"$task had a not serializable result: ${ef.description}; not retrying")
          abort(s"$task had a not serializable result: ${ef.description}")
          return
        }
        if (ef.className == classOf[TaskOutputFileAlreadyExistException].getName) {
          // If we can not write to output file in the task, there's no point in trying to
          // re-execute it.
          logError("Task %s in stage %s (TID %d) can not write to output file: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) can not write to output file: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost $task on ${info.host}, executor ${info.executorId}: " +
              s"${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case tk: TaskKilled =>
        // TaskKilled might have accumulator updates
        accumUpdates = tk.accums
        metricPeaks = tk.metricPeaks.toArray
        logWarning(failureReason)
        None

      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"${taskName(tid)} failed because while it was being computed, its executor " +
          "exited for a reason unrelated to the task. Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None

      case e: TaskFailedReason =>  // TaskResultLost and others
        logWarning(failureReason)
        None
    }

    if (tasks(index).isBarrier) {
      isZombie = true
    }

    sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, metricPeaks, info)

    if (!isZombie && reason.countTowardsTaskFailures) {
      assert (null != failureReason)
      taskSetExcludelistHelperOpt.foreach(_.updateExcludedForFailedTask(
        info.host, info.executorId, index, failureReason))
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }

    if (successful(index)) {
      logInfo(s"${taskName(info.taskId)} failed, but the task will not" +
        " be re-executed (either because the task failed with a shuffle data fetch failure," +
        " so the previous stage needs to be re-run, or because a different copy of the task" +
        " has already succeeded).")
    } else {
      // 重新提交执行
      addPendingTask(index)
    }

    maybeFinishTaskSet()
  }

  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   */
  def addRunningTask(tid: Long): Unit = {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it. */
  def removeRunningTask(tid: Long): Unit = {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def isSchedulable: Boolean = !isZombie &&
    (pendingTasks.all.nonEmpty || pendingSpeculatableTasks.all.nonEmpty)

  override def addSchedulable(schedulable: Schedulable): Unit = {}

  override def removeSchedulable(schedulable: Schedulable): Unit = {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String, reason: ExecutorLossReason): Unit = {
    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (isShuffleMapTasks && !env.blockManager.externalShuffleServiceEnabled && !isZombie) {
      // 遍历处理对应 Executor 上的 Task 列表
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = info.index
        // We may have a running task whose partition has been marked as successful,
        // this partition has another task completed in another stage attempt.
        // We treat it as a running task and will call handleFailedTask later.
        if (successful(index) && !info.running && !killedByOtherAttempt.contains(tid)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          // 将对应的 Task 重新注册等待被调度
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          // 通知对应的 Task 被 Resubmitted
          sched.dagScheduler.taskEnded(
            tasks(index), Resubmitted, null, Seq.empty, Array.empty, info)
        }
      }
    }

    // 遍历处理对应 Executor 上正在运行的 Task，标识已完成并重新提交执行
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      val exitCausedByApp: Boolean = reason match {
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled | ExecutorDecommission(_) => false
        case ExecutorProcessLost(_, _, false) => false
        case _ => true
      }
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp,
        Some(reason.toString)))
    }
    // recalculate valid locality levels and waits when executor is lost
    // 重新计算本地性
    recomputeLocality()
  }

  /**
   * Check if the task associated with the given tid has past the time threshold and should be
   * speculative run.
   */
  private def checkAndSubmitSpeculatableTask(
      tid: Long,
      currentTimeMillis: Long,
      threshold: Double): Boolean = {
    val info = taskInfos(tid)
    val index = info.index
    // 将任务加入到推断执行列表，并提交执行
    if (!successful(index) && copiesRunning(index) == 1 &&
        info.timeRunning(currentTimeMillis) > threshold && !speculatableTasks.contains(index)) {
      addPendingTask(index, speculatable = true)
      logInfo(
        ("Marking task %d in stage %s (on %s) as speculatable because it ran more" +
          " than %.0f ms(%d speculatable tasks in this taskset now)")
          .format(index, taskSet.id, info.host, threshold, speculatableTasks.size + 1))
      speculatableTasks += index
      sched.dagScheduler.speculativeTaskSubmitted(tasks(index))
      true
    } else {
      false
    }
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean = {
    // No need to speculate if the task set is zombie or is from a barrier stage. If there is only
    // one task we don't speculate since we don't have metrics to decide whether it's taking too
    // long or not, unless a task duration threshold is explicitly provided.
    if (isZombie || isBarrier || (numTasks == 1 && !speculationTaskDurationThresOpt.isDefined)) {
      return false
    }
    var foundTasks = false
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)

    // It's possible that a task is marked as completed by the scheduler, then the size of
    // `successfulTaskDurations` may not equal to `tasksSuccessful`. Here we should only count the
    // tasks that are submitted by this `TaskSetManager` and are completed successfully.
    val numSuccessfulTasks = successfulTaskDurations.size()
    // 当前集群已完成的 Task 数目超过阈值，则启动推断执行判断
    if (numSuccessfulTasks >= minFinishedForSpeculation) {
      val time = clock.getTimeMillis()
      // 获取任务执行成功时间的中位数
      val medianDuration = successfulTaskDurations.median
      // 计算推断时间阈值
      val threshold = max(speculationMultiplier * medianDuration, minTimeToSpeculation)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for (tid <- runningTasksSet) {
        // 判断 Task 是否满足推断执行，如果是则加入到推断执行列表，并提交执行
        var speculated = checkAndSubmitSpeculatableTask(tid, time, threshold)
        if (!speculated && executorDecommissionKillInterval.isDefined) {
          val taskInfo = taskInfos(tid)
          val decomState = sched.getExecutorDecommissionState(taskInfo.executorId)
          if (decomState.isDefined) {
            // Check if this task might finish after this executor is decommissioned.
            // We estimate the task's finish time by using the median task duration.
            // Whereas the time when the executor might be decommissioned is estimated using the
            // config executorDecommissionKillInterval. If the task is going to finish after
            // decommissioning, then we will eagerly speculate the task.
            val taskEndTimeBasedOnMedianDuration = taskInfos(tid).launchTime + medianDuration
            val executorDecomTime = decomState.get.startTime + executorDecommissionKillInterval.get
            val canExceedDeadline = executorDecomTime < taskEndTimeBasedOnMedianDuration
            if (canExceedDeadline) {
              speculated = checkAndSubmitSpeculatableTask(tid, time, 0)
            }
          }
        }
        foundTasks |= speculated
      }
    }
    // 当前集群已完成的 Task 数目未超过阈值
    else if (speculationTaskDurationThresOpt.isDefined && speculationTasksLessEqToSlots) {
      val time = clock.getTimeMillis()
      val threshold = speculationTaskDurationThresOpt.get
      logDebug(s"Tasks taking longer time than provided speculation threshold: $threshold")
      for (tid <- runningTasksSet) {
        foundTasks |= checkAndSubmitSpeculatableTask(tid, time, threshold)
      }
    }
    foundTasks
  }

  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    if (legacyLocalityWaitReset && isBarrier) return 0

    val localityWait = level match {
      case TaskLocality.PROCESS_LOCAL => config.LOCALITY_WAIT_PROCESS
      case TaskLocality.NODE_LOCAL => config.LOCALITY_WAIT_NODE
      case TaskLocality.RACK_LOCAL => config.LOCALITY_WAIT_RACK
      case _ => null
    }

    if (localityWait != null) {
      conf.get(localityWait)
    } else {
      0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   *
   * 计算有效可用的本地性级别
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    // 期望 PROCESS_LOCAL，且在同一个进程中存在活跃的 Executor
    if (!pendingTasks.forExecutor.isEmpty &&
        pendingTasks.forExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    // 期望 NODE_LOCAL，且在本节点上存在活跃的 Executor 进程
    if (!pendingTasks.forHost.isEmpty &&
        pendingTasks.forHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    // 期望 NO_PREF
    if (!pendingTasks.noPrefs.isEmpty) {
      levels += NO_PREF
    }
    // 期望 RACK_LOCAL，且在本机架上存在活跃的节点
    if (!pendingTasks.forRack.isEmpty &&
        pendingTasks.forRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    // ANY
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  def executorDecommission(execId: String): Unit = {
    recomputeLocality()
  }

  def recomputeLocality(): Unit = {
    // A zombie TaskSetManager may reach here while executorLost happens
    if (isZombie) return
    val previousLocalityIndex = currentLocalityIndex
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    val previousMyLocalityLevels = myLocalityLevels
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
    if (currentLocalityIndex > previousLocalityIndex) {
      // SPARK-31837: If the new level is more local, shift to the new most local locality
      // level in terms of better data locality. For example, say the previous locality
      // levels are [PROCESS, NODE, ANY] and current level is ANY. After recompute, the
      // locality levels are [PROCESS, NODE, RACK, ANY]. Then, we'll shift to RACK level.
      currentLocalityIndex = getLocalityIndex(myLocalityLevels.diff(previousMyLocalityLevels).head)
    }
  }

  def executorAdded(): Unit = {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KIB = 1000

  // 1 minute
  val BARRIER_LOGGING_INTERVAL = 60000
}

/**
 * Set of pending tasks for various levels of locality: executor, host, rack,
 * noPrefs and anyPrefs. These collections are actually
 * treated as stacks, in which new tasks are added to the end of the
 * ArrayBuffer and removed from the end. This makes it faster to detect
 * tasks that repeatedly fail because whenever a task failed, it is put
 * back at the head of the stack. These collections may contain duplicates
 * for two reasons:
 * (1): Tasks are only removed lazily; when a task is launched, it remains
 * in all the pending lists except the one that it was launched from.
 * (2): Tasks may be re-added to these lists multiple times as a result
 * of failures.
 * Duplicates are handled in dequeueTaskFromList, which ensures that a
 * task hasn't already started running before launching it.
 */
private[scheduler] class PendingTasksByLocality {

  // Set of pending tasks for each executor.
  val forExecutor = new HashMap[String, ArrayBuffer[Int]]
  // Set of pending tasks for each host. Similar to pendingTasksForExecutor, but at host level.
  val forHost = new HashMap[String, ArrayBuffer[Int]]
  // Set containing pending tasks with no locality preferences.
  val noPrefs = new ArrayBuffer[Int]
  // Set of pending tasks for each rack -- similar to the above.
  val forRack = new HashMap[String, ArrayBuffer[Int]]
  // Set containing all pending tasks (also used as a stack, as above).
  val all = new ArrayBuffer[Int]
}

private[scheduler] case class BarrierPendingLaunchTask(
    execId: String,
    host: String,
    index: Int,
    taskLocality: TaskLocality.TaskLocality,
    assignedResources: Map[String, ResourceInformation]) {
  // Stored the corresponding index of the WorkerOffer which is responsible to launch the task.
  // Used to revert the assigned resources (e.g., cores, custome resources) when the barrier
  // task set doesn't launch successfully in a single resourceOffers round.
  var assignedOfferIndex: Int = _
  var assignedCores: Int = 0
}
