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
package org.apache.spark.deploy.k8s.submit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Encapsulates arguments to the submission client.
 *
 * @param mainAppResource the main application resource if any
 * @param mainClass the main class of the application to run
 * @param driverArgs arguments to the driver
 */
private[spark] case class ClientArguments(
    // 任务 jar 文件路径，例如 local:///opt/spark/examples/jars/spark-examples_2.12-3.3.3.jar
    mainAppResource: MainAppResource,
    // 驱动类，例如 org.apache.spark.examples.SparkPi
    mainClass: String,
    driverArgs: Array[String],
    proxyUser: Option[String])

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: MainAppResource = JavaMainAppResource(None)
    var mainClass: Option[String] = None
    val driverArgs = mutable.ArrayBuffer.empty[String]
    var proxyUser: Option[String] = None

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = JavaMainAppResource(Some(primaryJavaResource))
      case Array("--primary-py-file", primaryPythonResource: String) =>
        mainAppResource = PythonMainAppResource(primaryPythonResource)
      case Array("--primary-r-file", primaryRFile: String) =>
        mainAppResource = RMainAppResource(primaryRFile)
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case Array("--proxy-user", user: String) =>
        proxyUser = Some(user)
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(mainClass.isDefined, "Main class must be specified via --main-class")

    ClientArguments(
      mainAppResource,
      mainClass.get,
      driverArgs.toArray,
      proxyUser)
  }
}

/**
 * Submits a Spark application to run on Kubernetes by creating the driver pod and starting a
 * watcher that monitors and logs the application status. Waits for the application to terminate if
 * spark.kubernetes.submission.waitAppCompletion is true.
 *
 * @param conf The kubernetes driver config.
 * @param builder Responsible for building the base driver pod based on a composition of
 *                implemented features.
 * @param kubernetesClient the client to talk to the Kubernetes API server
 * @param watcher a watcher that monitors and logs the application status
 */
private[spark] class Client(
    conf: KubernetesDriverConf,
    builder: KubernetesDriverBuilder,
    kubernetesClient: KubernetesClient,
    watcher: LoggingPodStatusWatcher) extends Logging {

  /**
   * 1. 构造 Driver 部署相关配置（ConfigMap、容器、Pod）
   * 2. 请求 k8s 创建 driver pod
   */
  def run(): Unit = {
    // 构造 Driver 部署配置
    val resolvedDriverSpec = builder.buildFromFeatures(conf, kubernetesClient)

    // 构造 ConfigMap，key 是文件名，value 是文件内容
    val configMapName = KubernetesClientUtils.configMapNameDriver
    val confFilesMap = KubernetesClientUtils.buildSparkConfDirFilesMap(configMapName,
      conf.sparkConf, resolvedDriverSpec.systemProperties)
    val configMap = KubernetesClientUtils.buildConfigMap(configMapName, confFilesMap +
        (KUBERNETES_NAMESPACE.key -> conf.namespace))

    // The include of the ENV_VAR for "SPARK_CONF_DIR" is to allow for the
    // Spark command builder to pickup on the Java Options present in the ConfigMap
    // 构造 Driver 容器配置
    val resolvedDriverContainer = new ContainerBuilder(resolvedDriverSpec.pod.container)
      .addNewEnv()
        .withName(ENV_SPARK_CONF_DIR)
        .withValue(SPARK_CONF_DIR_INTERNAL)
        .endEnv()
      .addNewVolumeMount()
        .withName(SPARK_CONF_VOLUME_DRIVER)
        .withMountPath(SPARK_CONF_DIR_INTERNAL)
        .endVolumeMount()
      .build()

    // 构建 Driver Pod 配置
    val resolvedDriverPod = new PodBuilder(resolvedDriverSpec.pod.pod)
      .editSpec()
        .addToContainers(resolvedDriverContainer)
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME_DRIVER)
          .withNewConfigMap()
            .withItems(KubernetesClientUtils.buildKeyToPathObjects(confFilesMap).asJava)
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()
    val driverPodName = resolvedDriverPod.getMetadata.getName

    // setup resources before pod creation
    val preKubernetesResources = resolvedDriverSpec.driverPreKubernetesResources
    try {
      kubernetesClient.resourceList(preKubernetesResources: _*).createOrReplace()
    } catch {
      case NonFatal(e) =>
        logError("Please check \"kubectl auth can-i create [resource]\" first." +
          " It should be yes. And please also check your feature step implementation.")
        kubernetesClient.resourceList(preKubernetesResources: _*).delete()
        throw e
    }

    var watch: Watch = null
    var createdDriverPod: Pod = null
    try {
      // 请求 k8s 创建 driver pod
      createdDriverPod = kubernetesClient.pods().create(resolvedDriverPod)
      if (log.isDebugEnabled) {
        logDebug(s"Create driver pod $createdDriverPod")
      }
    } catch {
      case NonFatal(e) =>
        kubernetesClient.resourceList(preKubernetesResources: _*).delete()
        logError("Please check \"kubectl auth can-i create pod\" first. It should be yes.")
        throw e
    }

    // Refresh all pre-resources' owner references
    try {
      addOwnerReference(createdDriverPod, preKubernetesResources)
      kubernetesClient.resourceList(preKubernetesResources: _*).createOrReplace()
    } catch {
      case NonFatal(e) =>
        kubernetesClient.pods().delete(createdDriverPod)
        kubernetesClient.resourceList(preKubernetesResources: _*).delete()
        throw e
    }

    // setup resources after pod creation, and refresh all resources' owner references
    try {
      val otherKubernetesResources = resolvedDriverSpec.driverKubernetesResources ++ Seq(configMap)
      addOwnerReference(createdDriverPod, otherKubernetesResources)
      kubernetesClient.resourceList(otherKubernetesResources: _*).createOrReplace()
    } catch {
      case NonFatal(e) =>
        kubernetesClient.pods().delete(createdDriverPod)
        throw e
    }

    if (conf.get(WAIT_FOR_APP_COMPLETION)) {
      val sId = Seq(conf.namespace, driverPodName).mkString(":")
      breakable {
        while (true) {
          val podWithName = kubernetesClient
            .pods()
            .withName(driverPodName)
          // Reset resource to old before we start the watch, this is important for race conditions
          watcher.reset()
          watch = podWithName.watch(watcher)

          // Send the latest pod state we know to the watcher to make sure we didn't miss anything
          watcher.eventReceived(Action.MODIFIED, podWithName.get())

          // Break the while loop if the pod is completed or we don't want to wait
          if (watcher.watchOrStop(sId)) {
            watch.close()
            break
          }
        }
      }
    }
  } // end of run
}

/**
 * Main class and entry point of application submission in KUBERNETES mode.
 */
private[spark] class KubernetesClientApplication extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    // 解析命令行参数，封装成 ClientArguments 对象
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    // 基于 k8s client 创建 Driver pod，启动 Spark application
    run(parsedArguments, conf)
  }

  private def run(clientArguments: ClientArguments, sparkConf: SparkConf): Unit = {
    // For constructing the app ID, we can't use the Spark application name, as the app ID is going
    // to be added as a label to group resources belonging to the same application. Label values are
    // considerably restrictive, e.g. must be no longer than 63 characters in length. So we generate
    // a unique app ID (captured by spark.app.id) in the format below.
    // 创建 applicationId
    val kubernetesAppId = KubernetesConf.getKubernetesAppId()
    // 构造 Driver 运行配置
    val kubernetesConf = KubernetesConf.createDriverConf(
      sparkConf,
      kubernetesAppId,
      clientArguments.mainAppResource,
      clientArguments.mainClass,
      clientArguments.driverArgs,
      clientArguments.proxyUser)
    // The master URL has been checked for validity already in SparkSubmit.
    // We just need to get rid of the "k8s://" prefix here.
    val master = KubernetesUtils.parseMasterUrl(sparkConf.get("spark.master"))
    val watcher = new LoggingPodStatusWatcherImpl(kubernetesConf)

    Utils.tryWithResource(
      // 创建 k8s 客户端 KubernetesClient 对象
      SparkKubernetesClientFactory.createKubernetesClient(
        master,
        Some(kubernetesConf.namespace),
        KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
        SparkKubernetesClientFactory.ClientType.Submission,
        sparkConf,
        None,
        None)) { kubernetesClient =>
      val client = new Client(
        kubernetesConf,
        new KubernetesDriverBuilder(),
        kubernetesClient,
        watcher)
      /*
       * 1. 构造 Driver 部署配置单（容器、ConfigMap、Pod）
       * 2. 请求 k8s 创建 driver pod
       */
      client.run()
    }
  }
}
