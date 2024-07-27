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

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}

/**
 * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {

  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)

  // Exposed for testing.
  protected val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")

  // Exposed for testing.
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }

  protected val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.serializer.newInstance()
    }
  }

  def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit = {
    // 会调用线程池 使用一个线程来执行下面的代码，正是因为调用线程执行了 更新完成任务的信息
    // 所以才会导致 针对该任务完成的对单个executor的任务调度已经开始了 但是任务的successful状态还没有更新好
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          // 将任务的结果反序列化 其中包括metrics的相关信息
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            // 如果数据是直接发送给driver的
            case directResult: DirectTaskResult[_] =>
              // 如果远端的数据超过了规定的最大值
              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              directResult.value(taskResultSerializer.get())
              (directResult, serializedData.limit())
            // 如果数据并不是直接发送给driver 而是返回block的元数据
            case IndirectTaskResult(blockId, size) =>
              if (!taskSetManager.canFetchMoreResults(size)) {
                // dropped by executor if size is larger than maxResultSize
                sparkEnv.blockManager.master.removeBlock(blockId)
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }

              logDebug(s"Fetching indirect task result for ${taskSetManager.taskName(tid)}")
              // 调用下面的方法表示正在获取任务的执行数据
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              //
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (serializedTaskResult.isEmpty) {
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // force deserialization of referenced value
              deserializedResult.value(taskResultSerializer.get())
              sparkEnv.blockManager.master.removeBlock(blockId)
              (deserializedResult, size)
          }

          // Set the task result size in the accumulator updates received from the executors.
          // We need to do this here on the driver because if we did this on the executors then
          // we would have to serialize the result again after updating the size.
          // 在driver端更新任务结果的大小信息
          // result为DirectTaskResult[_]类型 包含了task's return value, accumulator updates and metric peaks
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }

          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }

  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer): Unit = {
    var reason : TaskFailedReason = UnknownReason
    try {
      getTaskResultExecutor.execute(() => Utils.logUncaughtExceptions {
        val loader = Utils.getContextOrSparkClassLoader
        try {
          if (serializedData != null && serializedData.limit() > 0) {
            reason = serializer.get().deserialize[TaskFailedReason](
              serializedData, loader)
          }
        } catch {
          case _: ClassNotFoundException =>
            // Log an error but keep going here -- the task failed, so not catastrophic
            // if we can't deserialize the reason.
            logError(
              "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
          case _: Exception => // No-op
        } finally {
          // If there's an error while deserializing the TaskEndReason, this Runnable
          // will die. Still tell the scheduler about the task failure, to avoid a hang
          // where the scheduler thinks the task is still running.
          scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  // This method calls `TaskSchedulerImpl.handlePartitionCompleted` asynchronously. We do not want
  // DAGScheduler to call `TaskSchedulerImpl.handlePartitionCompleted` directly, as it's
  // synchronized and may hurt the throughput of the scheduler.
  def enqueuePartitionCompletionNotification(stageId: Int, partitionId: Int): Unit = {
    getTaskResultExecutor.execute(() => Utils.logUncaughtExceptions {
      scheduler.handlePartitionCompleted(stageId, partitionId)
    })
  }

  def stop(): Unit = {
    getTaskResultExecutor.shutdownNow()
  }
}
