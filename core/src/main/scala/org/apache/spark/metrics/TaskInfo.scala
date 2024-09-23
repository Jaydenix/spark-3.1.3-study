package org.apache.spark.metrics

import play.api.libs.json._

// 定义 InputMetrics 子结构
case class InputMetrics(bytesRead: Long, recordsRead: Long)
case class OutputMetrics(bytesWritten: Long, recordsWritten: Long)
case class ShuffleReadMetrics(
                               remoteBlocksFetched: Long,
                               localBlocksFetched: Long,
                               fetchWaitTime: Long,
                               remoteBytesRead: Long,
                               remoteBytesReadToDisk: Long,
                               localBytesRead: Long,
                               recordsRead: Long
                             )
case class ShuffleWriteMetrics(bytesWritten: Long, writeTime: Long, recordsWritten: Long)

// 定义 TaskMetrics 结构
case class TaskMetrics(
                        executorDeserializeTime: Long,
                        executorDeserializeCpuTime: Long,
                        executorRunTime: Long,
                        executorCpuTime: Long,
                        resultSize: Long,
                        jvmGcTime: Long,
                        resultSerializationTime: Long,
                        memoryBytesSpilled: Long,
                        diskBytesSpilled: Long,
                        peakExecutionMemory: Long,
                        inputMetrics: InputMetrics,
                        outputMetrics: OutputMetrics,
                        shuffleReadMetrics: ShuffleReadMetrics,
                        shuffleWriteMetrics: ShuffleWriteMetrics
                      )

// 定义 TaskInfo 结构
case class TaskInfo(
                     taskId: Long,
                     index: Int,
                     attempt: Int,
                     launchTime: String,
                     duration: Long,
                     executorId: String,
                     host: String,
                     status: String,
                     taskLocality: String,
                     speculative: Boolean,
                     accumulatorUpdates: Seq[String],
                     taskMetrics: TaskMetrics,
                     schedulerDelay: Long,
                     gettingResultTime: Long
                   )

object TaskInfo {
  implicit val inputMetricsFormat: OFormat[InputMetrics] = Json.format[InputMetrics]
  implicit val outputMetricsFormat: OFormat[OutputMetrics] = Json.format[OutputMetrics]
  implicit val shuffleReadMetricsFormat: OFormat[ShuffleReadMetrics] = Json.format[ShuffleReadMetrics]
  implicit val shuffleWriteMetricsFormat: OFormat[ShuffleWriteMetrics] = Json.format[ShuffleWriteMetrics]
  implicit val taskMetricsFormat: OFormat[TaskMetrics] = Json.format[TaskMetrics]
  implicit val taskInfoFormat: OFormat[TaskInfo] = Json.format[TaskInfo]
}

