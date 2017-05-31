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

import java.io.InputStream
import java.security.PrivilegedAction

import scala.async.Async
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

import com.linkedin.drelephant.security.HadoopSecurity
import com.linkedin.drelephant.spark.data.SparkLogDerivedData
import com.linkedin.drelephant.util.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerEnvironmentUpdate, SparkListenerEvent}
import org.json4s.{DefaultFormats, JsonAST}
import org.json4s.jackson.JsonMethods


/**
  * A client for getting data from the Spark event logs.
  */
class SparkLogClient(hadoopConfiguration: Configuration, sparkConf: SparkConf, eventLogUri: Option[String]) {
  import SparkLogClient._
  import Async.async

  private val logger: Logger = Logger.getLogger(classOf[SparkLogClient])

  private lazy val security: HadoopSecurity = new HadoopSecurity()

  protected lazy val sparkUtils: SparkUtils = SparkUtils

  def fetchData(appId: String, attemptId: Option[String])(implicit ec: ExecutionContext): Future[SparkLogDerivedData] =
    doAsPrivilegedAction { () => doFetchData(appId, attemptId) }

  protected def doAsPrivilegedAction[T](action: () => T): T =
    security.doAs[T](new PrivilegedAction[T] { override def run(): T = action() })

  protected def doFetchData(
    appId: String,
    attemptId: Option[String]
  )(
    implicit ec: ExecutionContext
  ): Future[SparkLogDerivedData] = {
    val (eventLogFileSystem, baseEventLogPath) =
      sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, eventLogUri)
    val (eventLogPath, eventLogCodec) =
      sparkUtils.pathAndCodecforEventLog(sparkConf, eventLogFileSystem, baseEventLogPath, appId, attemptId)

    async {
      sparkUtils.withEventLog(eventLogFileSystem, eventLogPath, eventLogCodec)(findDerivedData(_))
    }
  }
}

object SparkLogClient {
  def findDerivedData(in: InputStream, eventsLimit: Option[Int] = None): SparkLogDerivedData = {
    val parser = new SparkEventLogParser()
    parser.load(in)
    println("EnvDetails: " + parser.getEnvironmentDetails )
    parser.getEnvironmentDetails
      .map(SparkLogDerivedData(_))
      .getOrElse { throw new IllegalArgumentException("Spark event log doesn't have Spark properties") }
  }
}
