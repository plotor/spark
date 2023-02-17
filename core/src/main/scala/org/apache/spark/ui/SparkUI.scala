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

package org.apache.spark.ui

import java.util.Date

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.EnvironmentTab
import org.apache.spark.ui.exec.ExecutorsTab
import org.apache.spark.ui.jobs.{JobsTab, StagesTab}
import org.apache.spark.ui.storage.StorageTab

/**
 * Top level user interface for a Spark application.
 */
private[spark] class SparkUI private (
    val store: AppStatusStore,
    val sc: Option[SparkContext],
    val conf: SparkConf,
    securityManager: SecurityManager,
    var appName: String,
    val basePath: String,
    val startTime: Long,
    val appSparkVersion: String)
  extends WebUI(securityManager, securityManager.getSSLOptions("ui"), SparkUI.getUIPort(conf),
    conf, basePath, "SparkUI")
  with Logging
  with UIRoot {

  // 是否提供 Kill Stage 或 Job 的链接
  val killEnabled: Boolean = sc.exists(_.conf.get(UI_KILL_ENABLED))

  // 当前应用 ID
  var appId: String = _

  private var streamingJobProgressListener: Option[SparkListener] = None

  /** Initialize all components of the server. */
  def initialize(): Unit = {
    // 添加 Jobs Tab
    val jobsTab = new JobsTab(this, store)
    attachTab(jobsTab)

    // 添加 Stages Tab
    val stagesTab = new StagesTab(this, store)
    attachTab(stagesTab)

    // 添加 Storage Tab
    attachTab(new StorageTab(this, store))

    // 添加 Environment Tab
    attachTab(new EnvironmentTab(this, store))

    // 添加 Executors Tab
    attachTab(new ExecutorsTab(this))

    addStaticHandler(SparkUI.STATIC_RESOURCE_DIR)

    // 添加重定向 Handler，将对于 / 路径的访问重定向到 /job
    attachHandler(createRedirectHandler("/", "/jobs/", basePath = basePath))

    attachHandler(ApiRootResource.getServletHandler(this))

    if (sc.exists(_.conf.get(UI_PROMETHEUS_ENABLED))) {
      attachHandler(PrometheusResource.getServletHandler(this))
    }

    // These should be POST only, but, the YARN AM proxy won't proxy POSTs
    attachHandler(createRedirectHandler(
      "/jobs/job/kill", "/jobs/", jobsTab.handleKillRequest, httpMethods = Set("GET", "POST")))
    attachHandler(createRedirectHandler(
      "/stages/stage/kill", "/stages/", stagesTab.handleKillRequest, httpMethods = Set("GET", "POST")))
  }

  // 构造期间执行初始化逻辑
  initialize()

  def getSparkUser: String = {
    try {
      Option(store.applicationInfo().attempts.head.sparkUser)
        .orElse(store.environmentInfo().systemProperties.toMap.get("user.name"))
        .getOrElse("<unknown>")
    } catch {
      case _: NoSuchElementException => "<unknown>"
    }
  }

  def getAppName: String = appName

  def setAppId(id: String): Unit = {
    appId = id
  }

  /** Stop the server behind this web interface. Only valid after bind(). */
  override def stop(): Unit = {
    super.stop()
    logInfo(s"Stopped Spark web UI at $webUrl")
  }

  override def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
    if (appId == this.appId) {
      fn(this)
    } else {
      throw new NoSuchElementException()
    }
  }

  override def checkUIViewPermissions(appId: String, attemptId: Option[String],
      user: String): Boolean = {
    securityManager.checkUIViewPermissions(user)
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    Iterator(new ApplicationInfo(
      id = appId,
      name = appName,
      coresGranted = None,
      maxCores = None,
      coresPerExecutor = None,
      memoryPerExecutorMB = None,
      attempts = Seq(new ApplicationAttemptInfo(
        attemptId = None,
        startTime = new Date(startTime),
        endTime = new Date(-1),
        duration = 0,
        lastUpdated = new Date(startTime),
        sparkUser = getSparkUser,
        completed = false,
        appSparkVersion = appSparkVersion
      ))
    ))
  }

  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    getApplicationInfoList.find(_.id == appId)
  }

  def getStreamingJobProgressListener: Option[SparkListener] = streamingJobProgressListener

  def setStreamingJobProgressListener(sparkListener: SparkListener): Unit = {
    streamingJobProgressListener = Option(sparkListener)
  }

  def clearStreamingJobProgressListener(): Unit = {
    streamingJobProgressListener = None
  }
}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.appName

  def appSparkVersion: String = parent.appSparkVersion
}

private[spark] object SparkUI {
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  val DEFAULT_POOL_NAME = "default"

  def getUIPort(conf: SparkConf): Int = {
    conf.get(UI_PORT)
  }

  /**
   * Create a new UI backed by an AppStatusStore.
   */
  def create(
      sc: Option[SparkContext],
      store: AppStatusStore,
      conf: SparkConf,
      securityManager: SecurityManager,
      appName: String,
      basePath: String,
      startTime: Long,
      appSparkVersion: String = org.apache.spark.SPARK_VERSION): SparkUI = {

    new SparkUI(store, sc, conf, securityManager, appName, basePath, startTime, appSparkVersion)
  }

}
