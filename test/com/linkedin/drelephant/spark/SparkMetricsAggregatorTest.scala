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

package com.linkedin.drelephant.spark

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.{Map, JavaConverters}

import com.linkedin.drelephant.analysis.ApplicationType
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.status.api.v1.StageStatus

class SparkMetricsAggregatorTest extends FunSpec with Matchers {
  import SparkMetricsAggregatorTest._

  describe("SparkMetricsAggregator") {
    val aggregatorConfigurationData = newFakeAggregatorConfigurationData(
      Map("allocated_memory_waste_buffer_percentage" -> "0.5")
    )

    val appId = "application_1"

    val applicationInfo = {
      val applicationAttemptInfo = {
        val now = System.currentTimeMillis
        val duration = 8000000L
        newFakeApplicationAttemptInfo(Some("1"), startTime = new Date(now - duration), endTime = new Date(now))
      }
      new ApplicationInfo(appId, name = "app", Seq(applicationAttemptInfo))
    }

    val restDerivedData = {
      val executorSummaries = Seq(
        newFakeExecutorSummary(id = "1", totalDuration = 1000000L),
        newFakeExecutorSummary(id = "2", totalDuration = 3000000L)
      )
      val taskDatas = Seq(
        newFakeTaskData(1, 500000L, 1000000L, 500000L)
      )
      val stageDatas = Seq(
        newFakeStageData(id = 1 , taskDatas = taskDatas)
      )
      SparkRestDerivedData(
        applicationInfo,
        jobDatas = Seq.empty,
        stageDatas = stageDatas,
        executorSummaries = executorSummaries
      )
    }

    describe("when it has log-derived data") {
      val logDerivedData = {
        val environmentUpdate = newFakeSparkListenerEnvironmentUpdate(
          Map(
            "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
            "spark.storage.memoryFraction" -> "0.3",
            "spark.driver.memory" -> "2G",
            "spark.executor.instances" -> "2",
            "spark.executor.memory" -> "4g",
            "spark.shuffle.memoryFraction" -> "0.5"
          )
        )
        SparkLogDerivedData(environmentUpdate)
      }

      val data = SparkApplicationData(appId, restDerivedData, Some(logDerivedData))

      val aggregator = new SparkMetricsAggregator(aggregatorConfigurationData)
      aggregator.aggregate(data)

      val result = aggregator.getResult

      it("calculates resources used") {
        val totalExecutorTimeSeconds = 1000 + 3000
        val executorMemoryMb = 4096
        result.getResourceUsed should be(executorMemoryMb * totalExecutorTimeSeconds)
      }

      it("calculates resources wasted") {
        val executorMemoryMb = 4096
        val totalTaskTimeSeconds = 2000
        val totalExecutorTimeSeconds = 1000 + 3000
        val resourceAllocated = executorMemoryMb * totalExecutorTimeSeconds;

        val resourceUsed = executorMemoryMb * totalTaskTimeSeconds;


        result.getResourceWasted should be(resourceAllocated - resourceUsed * 1.5)
      }

      it("doesn't calculate total delay") {
        result.getTotalDelay should be(0L)
      }
    }

    describe("when it doesn't have log-derived data") {
      val data = SparkApplicationData(appId, restDerivedData, logDerivedData = None)

      val aggregator = new SparkMetricsAggregator(aggregatorConfigurationData)
      aggregator.aggregate(data)

      val result = aggregator.getResult

      it("doesn't calculate resources used") {
        result.getResourceUsed should be(0L)
      }

      it("doesn't calculate resources wasted") {
        result.getResourceWasted should be(0L)
      }

      it("doesn't calculate total delay") {
        result.getTotalDelay should be(0L)
      }
    }
  }
}

object SparkMetricsAggregatorTest {
  import JavaConverters._

  def newFakeAggregatorConfigurationData(params: Map[String, String] = Map.empty): AggregatorConfigurationData =
      new AggregatorConfigurationData("org.apache.spark.SparkMetricsAggregator", new ApplicationType("SPARK"), params.asJava)

  def newFakeSparkListenerEnvironmentUpdate(appConfigurationProperties: Map[String, String]): SparkListenerEnvironmentUpdate =
    SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq))

  def newFakeApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Date,
    endTime: Date
  ): ApplicationAttemptInfo = new ApplicationAttemptInfo(
    attemptId,
    startTime,
    endTime,
    sparkUser = "foo",
    completed = true
  )

  def newFakeExecutorSummary(
    id: String,
    totalDuration: Long
  ): ExecutorSummary = new ExecutorSummary(
    id,
    hostPort = "",
    rddBlocks = 0,
    memoryUsed = 0,
    diskUsed = 0,
    activeTasks = 0,
    failedTasks = 0,
    completedTasks = 0,
    totalTasks = 0,
    totalDuration,
    totalInputBytes = 0,
    totalShuffleRead = 0,
    totalShuffleWrite = 0,
    maxMemory = 0,
    executorLogs = Map.empty
  )

  def newFakeTaskData(id: Long,
                      executorDeserializeTime: Long,
                      executorRunTime: Long,
                      resultSerializationTime: Long) : TaskData = new TaskData(
    taskId = id,
    index = 0,
    attempt = 0,
    launchTime = new SimpleDateFormat("yyyy-MM-dd").parse("2014-02-11"),
    executorId = "",
    host = "",
    taskLocality = "",
    speculative = false,
    accumulatorUpdates = Seq.empty,
    errorMessage = None,
    new Some(new TaskMetrics(
      executorDeserializeTime = executorDeserializeTime,
      executorRunTime = executorRunTime,
      resultSize = 0L,
      jvmGcTime = 0L,
      resultSerializationTime = resultSerializationTime,
      memoryBytesSpilled = 0L,
      diskBytesSpilled = 0L,
      inputMetrics = None,
      outputMetrics = None,
      shuffleReadMetrics = None,
      shuffleWriteMetrics = None
    ))
  )

  def newFakeStageData(id: Int, taskDatas: Seq[TaskData]) = new StageData(
    status = StageStatus.COMPLETE,
    stageId = id,
    attemptId = 0,
    numActiveTasks = 0,
    numCompleteTasks = 0,
    numFailedTasks = 0,
    executorRunTime = 0L,
    inputBytes = 0L,
    inputRecords = 0L,
    outputBytes = 0L,
    outputRecords = 0L,
    shuffleReadBytes = 0L,
    shuffleReadRecords = 0L,
    shuffleWriteBytes = 0L,
    shuffleWriteRecords = 0L,
    memoryBytesSpilled = 0L,
    diskBytesSpilled = 0L,
    name = "",
    details = "",
    schedulingPool = "",
    accumulatorUpdates = Seq.empty,
    tasks = Some(taskDatas.map(taskData => taskData.taskId -> taskData).toMap),
    executorSummary = None
  )
}
