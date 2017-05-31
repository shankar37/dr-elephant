/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.fetchers

import com.linkedin.drelephant.analysis.ApplicationType
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import java.io.InputStream
import java.util.{Set => JSet, Properties, List => JList, HashSet => JHashSet, ArrayList => JArrayList, Date}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{ReplayListenerBus, StageInfo}
import org.apache.spark.storage.{RDDInfo, StorageStatus}
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.scheduler._
import org.json4s.{DefaultFormats, JsonAST}
import org.json4s.jackson.JsonMethods
import scala.collection.mutable
import scala.io.Source

/**
 * This class wraps the logic of collecting the data from sparkEventLog into the
 * SparkApplicationData instances.
 */
class SparkEventLogParser {
  import JsonAST._
  /** begin functions copied from jsonprotocol.scala from the spark code base **/
  private object SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES {
    val stageSubmitted = getFormattedClassName(SparkListenerStageSubmitted)
    val stageCompleted = getFormattedClassName(SparkListenerStageCompleted)
    val taskStart = getFormattedClassName(SparkListenerTaskStart)
    val taskGettingResult = getFormattedClassName(SparkListenerTaskGettingResult)
    val taskEnd = getFormattedClassName(SparkListenerTaskEnd)
    val jobStart = getFormattedClassName(SparkListenerJobStart)
    val jobEnd = getFormattedClassName(SparkListenerJobEnd)
    val environmentUpdate = getFormattedClassName(SparkListenerEnvironmentUpdate)
    val blockManagerAdded = getFormattedClassName(SparkListenerBlockManagerAdded)
    val blockManagerRemoved = getFormattedClassName(SparkListenerBlockManagerRemoved)
    val unpersistRDD = getFormattedClassName(SparkListenerUnpersistRDD)
    val applicationStart = getFormattedClassName(SparkListenerApplicationStart)
    val applicationEnd = getFormattedClassName(SparkListenerApplicationEnd)
    val executorAdded = getFormattedClassName(SparkListenerExecutorAdded)
    val executorRemoved = getFormattedClassName(SparkListenerExecutorRemoved)
    //val logStart = getFormattedClassName(SparkListenerLogStart)
    val metricsUpdate = getFormattedClassName(SparkListenerExecutorMetricsUpdate)
  }

  private def getFormattedClassName(obj: AnyRef): String = obj.getClass.getSimpleName.replace("$", "")

  private def mapFromJson(json: JValue): Map[String, String] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.map { case JField(k, JString(v)) => (k, v) }.toMap
  }

  /** Return an option that translates JNothing to None */
  private def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }
  /** end functions copied from jsonprotocol.scala from the spark code base **/

  import SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES._
  private implicit val formats: DefaultFormats = DefaultFormats
  private lazy val logger = Logger.getLogger(getClass)

  private var _applicationStart : Option[SparkListenerApplicationStart] = None
  private var _applicationInfo : Option[ApplicationInfoImpl] = None;
  private var _currentApplicationAttemptInfo : Option[ApplicationAttemptInfoImpl] = None
  private var _environmentDetails : Option[Map[String, String]] = None
  private var _executorSummaries = new mutable.HashMap[String, ExecutorSummaryImpl]
  private var _currentExecutorSummary : Option[ExecutorSummaryImpl] = None

  def getEnvironmentDetails : Option[Map[String, String]] = _environmentDetails

  def load(in: InputStream): Unit = {
    val events = parseStream(in)
  }

  private def parseStream(in: InputStream): Unit = {
    var lines = getLines(in)
    lines.foreach(parseLine(_))
  }

  private def getLines(in: InputStream): Iterator[String] = Source.fromInputStream(in).getLines

  private def parseLine(line: String): Unit = {
    println("Parsing Line " + line)
    parseEventJson(JsonMethods.parse(line))
  }

  private def parseEventJson(json: JValue): Unit = {
    println("Parsing Event : " + (json \ "Event").extract[String] )
    (json \ "Event").extract[String] match {
      case `environmentUpdate` => parseEnvironmentDetails(json)
      case `applicationStart` => parseApplicationStart(json)
      case `applicationEnd` =>   parseApplicationEnd(json); None
      case `executorAdded` =>   parseExecutorAdded(json); None
      case `executorRemoved` =>   parseExecutorRemoved(json); None
      case `jobStart` =>   dumpJson(json); None
      case `jobEnd` =>   dumpJson(json); None
      case `stageSubmitted` =>   dumpJson(json); None
      case `stageCompleted` =>   dumpJson(json); None
      case `taskStart` =>   dumpJson(json); None
      case `taskEnd` =>   dumpJson(json); None
      case _ =>  None
    }
  }


  private def dumpJson(json: JValue): Unit = {
    logger.info("event detected: " + json.toString)
  }
  private def parseApplicationEnd(json: JValue): Unit = {
    var attemptId = (json \ "App Attempt ID").extractOrElse[String]("")
    var attempt = _applicationInfo.map(_.attempts.find(
      attemptInfo => attemptInfo.attemptId.equals(attemptId)))
    attempt match {
      case Some(attemptInfo: ApplicationAttemptInfoImpl) => attemptInfo.endTime = new Date((json \ "Timestamp").extract[Long])
      case None => logger.warn("ApplicationEnd came without an active ApplicationStart")
    }
  }


  private def parseApplicationStart(json: JValue): Unit = {
    logger.info("detected applicationStart for app:  " + json \ "App Name")
    var appName = (json \ "App Name").extract[String]
    var appId = (json \ "App ID").extract[String]
    var attemptId = (json \ "App Attempt ID").extractOrElse[String]("")
    var user = (json \ "User").extract[String]
    var timeStamp = (json \ "Timestamp").extract[Long]

    if(_applicationInfo.isDefined) {
      throw new RuntimeException("Multiple Application Start Encountered")
    }
    var applicationInfo = new ApplicationInfoImpl()
    applicationInfo.name = appName
    applicationInfo.id = appId
    var applicationAttemptInfo = new ApplicationAttemptInfoImpl()
    applicationAttemptInfo.attemptId = attemptId match { case "" => None; case _ => Some(attemptId)}
    applicationAttemptInfo.startTime = new Date(timeStamp)
    applicationAttemptInfo.sparkUser = user
    applicationAttemptInfo.completed = false
    applicationInfo.attempts = new mutable.MutableList[ApplicationAttemptInfoImpl]()
    applicationInfo.attempts = applicationInfo.attempts :+ applicationAttemptInfo

  }

  private def parseEnvironmentDetails(json: JValue): Unit = {
    println("detected environmentUpdate for app:  " + json \ "App Name")
    _environmentDetails = Some(mapFromJson(json \ "Spark Properties"))
  }

  private var _executorStartTime = new mutable.HashMap[String, Long]()

  def parseExecutorAdded(json: JValue): Unit = {
    val time = (json \ "Timestamp").extract[Long]
    val executorId = (json \ "Executor ID").extract[String]

    var executorSummary = new ExecutorSummaryImpl(executorId)
    _executorSummaries.update(executorId, executorSummary)
    _executorStartTime.update(executorId, time)
  }

  def parseExecutorRemoved(json: JValue): Unit = {
    val time = (json \ "Timestamp").extract[Long]
    val executorId = (json \ "Executor ID").extract[String]
    _executorSummaries.get(executorId).foreach(_.totalDuration =  _executorStartTime.get(executorId).getOrElse(time) - time)
  }}
