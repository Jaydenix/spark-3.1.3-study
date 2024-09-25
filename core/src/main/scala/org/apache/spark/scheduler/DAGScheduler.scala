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

// import scala.language.existentials

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.{Map, mutable}
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.concurrent.duration._
import scala.util.control.NonFatal
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests.TEST_NO_STAGE_RETRY
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.{HadoopRDD, NewHadoopRDD, RDD, RDDCheckpointData, ShuffledRDD}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile.{DEFAULT_RESOURCE_PROFILE_ID, EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.{BlockLocationsAndStatus, BlockManagerHeartbeat}
import org.apache.spark.util._


/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * When looking through this code, there are several key concepts:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
 *
 *  - Tasks are individual units of work, each sent to one machine.
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark] class DAGScheduler(
                                   private[scheduler] val sc: SparkContext,
                                   // DAGScheduler包括TaskScheduler
                                   private[scheduler] val taskScheduler: TaskScheduler,
                                   listenerBus: LiveListenerBus,
                                   mapOutputTracker: MapOutputTrackerMaster,
                                   blockManagerMaster: BlockManagerMaster,
                                   env: SparkEnv,
                                   clock: Clock = new SystemClock())
  extends Logging {
  logInfo("=====3.成员包括 TaskScheduler Stages=====")
  logInfo("=====3.内容包括 Stage operations=====")

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  /**
   * Custom modifications by jaken
   * 定义stage提前获得各个任务的输入数据变量 rddToPartitionSize,key=rdd的名称 value=[partition0Size,partition1Size,...]
   * 当有RDD被缓存的时候 当前taskSet的输入就是之前被缓存的RDD对应的taskSet的输入 所以在获取shuffleStatus之前,需要判断是否被缓存
   */
  // private val rddToPartitionSize: mutable.Map[String, Array[Long]] = new mutable.HashMap[String, Array[Long]]()

  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)

  private[scheduler] def numTotalJobs: Int = nextJobId.get()

  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
   */
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   */
  // 缓存的位置 [rdd.id,partition[partitionLocs]]
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]
  /**
   * Custom modifications by jaken
   * 自定义变量 获得缓存的位置和大小
   */
  private val cacheLocsAndSizes = new mutable.HashMap[Int, IndexedSeq[(Seq[TaskLocation], Seq[Long])]]

  /**
   * Custom modifications by jaken
   * 定义缓存hadoopRDD各个分区大小的变量 [rdd.id,partitionSize]
   */
  private val cacheHadoopRDDsize = new mutable.HashMap[Int, mutable.IndexedSeq[Long]]

  /**
   * Custom modifications by jaken
   * 定义三个变量 分别表示getPreferredLocsAndSizes的数据从哪来
   * 调用当前RDD是否已经缓存
   */
  private var fromCached = false
  private var fromShuffle = false
  private var fromHDFS = false

  /**
   * Tracks the latest epoch of a fully processed error related to the given executor. (We use
   * the MapOutputTracker's epoch number, which is sent with every task.)
   *
   * When an executor fails, it can affect the results of many tasks, and we have to deal with
   * all of them consistently. We don't simply ignore all future results from that executor,
   * as the failures may have been transient; but we also don't want to "overreact" to follow-
   * on errors we receive. Furthermore, we might receive notification of a task success, after
   * we find out the executor has actually failed; we'll assume those successes are, in fact,
   * simply delayed notifications and the results have been lost, if the tasks started in the
   * same or an earlier epoch. In particular, we use this to control when we tell the
   * BlockManagerMaster that the BlockManager has been lost.
   */
  private val executorFailureEpoch = new HashMap[String, Long]

  /**
   * Tracks the latest epoch of a fully processed error where shuffle files have been lost from
   * the given executor.
   *
   * This is closely related to executorFailureEpoch. They only differ for the executor when
   * there is an external shuffle service serving shuffle files and we haven't been notified that
   * the entire worker has been lost. In that case, when an executor is lost, we do not update
   * the shuffleFileLostEpoch; we wait for a fetch failure. This way, if only the executor
   * fails, we do not unregister the shuffle data as it can still be served; but if there is
   * a failure in the shuffle service (resulting in fetch failure), we unregister the shuffle
   * data only once, even if we get many fetch failures.
   */
  private val shuffleFileLostEpoch = new HashMap[String, Long]

  private[scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.get(TEST_NO_STAGE_RETRY)

  private val shouldMergeResourceProfiles = sc.getConf.get(config.RESOURCE_PROFILE_MERGE_CONFLICTS)

  /**
   * Whether to unregister all the outputs on the host in condition that we receive a FetchFailure,
   * this is set default to false, which means, we only unregister the outputs related to the exact
   * executor(instead of the host) on a FetchFailure.
   */
  private[scheduler] val unRegisterOutputOnHostOnFetchFailure =
    sc.getConf.get(config.UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE)

  /**
   * Number of consecutive stage attempts allowed before a stage is aborted.
   */
  private[scheduler] val maxConsecutiveStageAttempts =
    sc.getConf.getInt("spark.stage.maxConsecutiveAttempts",
      DAGScheduler.DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS)

  /**
   * Number of max concurrent tasks check failures for each barrier job.
   */
  private[scheduler] val barrierJobIdToNumTasksCheckFailures = new ConcurrentHashMap[Int, Int]

  /**
   * Time in seconds to wait between a max concurrent tasks check failure and the next check.
   */
  private val timeIntervalNumTasksCheck = sc.getConf
    .get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL)

  /**
   * Max number of max concurrent tasks check failures allowed for a job before fail the job
   * submission.
   */
  private val maxFailureNumTasksCheck = sc.getConf
    .get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  // 内部有DAGSchedulerEventProcessLoop成员变量
  private[spark] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(sc.getConf)

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {
    // 向事件队列中发送BeginEvent事件
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
                 task: Task[_],
                 reason: TaskEndReason,
                 result: Any,
                 accumUpdates: Seq[AccumulatorV2[_, _]],
                 metricPeaks: Array[Long],
                 taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, metricPeaks, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
                                 execId: String,
                                 // (taskId, stageId, stageAttemptId, accumUpdates)
                                 accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
                                 blockManagerId: BlockManagerId,
                                 // (stageId, stageAttemptId) -> metrics
                                 executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates,
      executorUpdates))
    blockManagerMaster.driverHeartbeatEndPoint.askSync[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(10.minutes, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a worker is removed.
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit = {
    eventProcessLoop.post(WorkerRemoved(workerId, host, message))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  /**
   * Called by the TaskSetManager when it decides a speculative task is needed.
   */
  def speculativeTaskSubmitted(task: Task[_]): Unit = {
    eventProcessLoop.post(SpeculativeTaskSubmitted(task))
  }

  /**
   * Called by the TaskSetManager when a taskset becomes unschedulable due to executors being
   * excluded because of too many task failures and dynamic allocation is enabled.
   */
  def unschedulableTaskSetAdded(
                                 stageId: Int,
                                 stageAttemptId: Int): Unit = {
    eventProcessLoop.post(UnschedulableTaskSetAdded(stageId, stageAttemptId))
  }

  /**
   * Called by the TaskSetManager when an unschedulable taskset becomes schedulable and dynamic
   * allocation is enabled.
   */
  def unschedulableTaskSetRemoved(
                                   stageId: Int,
                                   stageAttemptId: Int): Unit = {
    eventProcessLoop.post(UnschedulableTaskSetRemoved(stageId, stageAttemptId))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        // 获取当前rdd的所有partition对应的blockIds
        val blockIds = rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  /**
   * Custom modifications by jaken
   * 获得缓存的位置和大小
   */
  private[scheduler]
  def getCacheLocsAndSizes(rdd: RDD[_]): IndexedSeq[(Seq[TaskLocation], Seq[Long])] = cacheLocsAndSizes.synchronized {
    // logInfo(s"#####cacheLocsAndSizes=${cacheLocsAndSizes.mkString("\n")} #####")
    // 获得blockManager
    val blockManager = SparkEnv.get.blockManager
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    // 表明当前RDD还没有缓存记录 下面会尝试通过blockManager寻找它的位置
    if (!cacheLocsAndSizes.contains(rdd.id)
    //      ||
    //      (cacheLocsAndSizes.contains(rdd.id) && cacheLocsAndSizes(rdd.id).exists {
    //        case (locs, sizes) => locs.isEmpty || sizes.isEmpty
    //      })
    ) {
      logInfo(s"#####当前RDD=${rdd.id}不在cacheLocsAndSizes.keys=${cacheLocsAndSizes.keySet.mkString("[", ",", "]")}中#####")
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locsAndSizes: IndexedSeq[(Seq[TaskLocation], Seq[Long])] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)((Nil, Nil))
      } else {
        // 获取当前rdd的所有partition对应的blockIds
        val blockIds = rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        logInfo(s"#####blockIds=${blockIds.head}, ... , ${blockIds.last} #####")
        /*     blockManagerMaster.getLocations(blockIds).map { bms =>
                  bms.map(bm => TaskLocation(bm.host, bm.executorId))
                }*/

        val locsAndSizes: IndexedSeq[(Seq[TaskLocation], Seq[Long])] = blockIds.map { blockId =>
          // 获取 blockLocationsAndStatus 其中blockStatus中包括块占用的内存大小和磁盘大小
          val blockLocationsAndStatus: Option[BlockLocationsAndStatus] =
            blockManagerMaster.getLocationsAndStatus(blockId, blockManager.blockManagerId.host)
          // if (blockLocationsAndStatus.isEmpty) logInfo(s"#####查询不到当前blockId=${blockId}的位置和状态信息#####")
          val locsAndSizePerBlock: (Seq[TaskLocation], Seq[Long]) = blockLocationsAndStatus.map { bls =>
            val blockStatus: BlockStatus = bls.status
            val blockLocations: Seq[BlockManagerId] = bls.locations
            // 将 blockLocations 转换为 TaskLocation
            val taskLocations: Seq[TaskLocation] = blockLocations.map { bm =>
              TaskLocation(bm.host, bm.executorId)
            }
            // 最终返回一个 (Seq[TaskLocation], Seq[Long]) 对
            (taskLocations, Seq(blockStatus.diskSize + blockStatus.memSize))
          }.getOrElse((Nil, Nil))
          // logInfo(s"#####locsAndSize = ${locsAndSizePerBlock} #####")
          locsAndSizePerBlock
        }
        logInfo(s"缓存的数据和位置: locsAndSizes = ${locsAndSizes.head}, ... ,${locsAndSizes.last}")
        locsAndSizes
      }
      cacheLocsAndSizes(rdd.id) = locsAndSizes
    }
    cacheLocsAndSizes(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()

    /**
     * Custom modifications by jaken
     * 这里投机取巧一下 清除缓存位置的同时顺便把hadoopRDD各个分区的大小也清除了
     */
    // logInfo(s"#####cacheLocsAndSizes=${cacheLocsAndSizes.mkString("Array(", ", ", ")")}#####")
    cacheLocsAndSizes.clear()
    logInfo("=====调用clearCacheLocs清除cacheLocsAndSizes.keys=${cacheLocsAndSizes.keySet.mkString(\"[\",\",\",\"]\")}" +
      "(仅存放当前stage中已经访问的RDD，在进入新stage前会被清除) =====")
  }

  /**
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
   */
  private def getOrCreateShuffleMapStage(
                                          shuffleDep: ShuffleDependency[_, _, _],
                                          firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        // 获得所有的shuffle依赖     RDD1 -> RDD2 -> RDD3 -> RDD4
        // 传递的参数                                 ↑
        // 获得依赖并创建shuffleMapStage  ↑       ↑
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          // shuffleId是倒序的 比如: RDD1 -> RDD2 -> RDD3 shuffleId依次是1 0
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // 最后再生成最后依赖的stage
        // 即: RDD1 -> RDD2 -> RDD3 -> RDD4
        //                          ↑
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
   * Check to make sure we don't launch a barrier stage with unsupported RDD chain pattern. The
   * following patterns are not supported:
   * 1. Ancestor RDDs that have different number of partitions from the resulting RDD (e.g.
   * union()/coalesce()/first()/take()/PartitionPruningRDD);
   * 2. An RDD that depends on multiple barrier RDDs (e.g. barrierRdd1.zip(barrierRdd2)).
   */
  private def checkBarrierStageWithRDDChainPattern(rdd: RDD[_], numTasksInStage: Int): Unit = {
    if (rdd.isBarrier() &&
      !traverseParentRDDsWithinStage(rdd, (r: RDD[_]) =>
        r.getNumPartitions == numTasksInStage &&
          r.dependencies.count(_.rdd.isBarrier()) <= 1)) {
      throw new BarrierJobUnsupportedRDDChainException
    }
  }

  /**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
   */
  def createShuffleMapStage[K, V, C](
                                      shuffleDep: ShuffleDependency[K, V, C], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    // 只获得当前rdd的直系shuffle依赖 比如 A - B - C --D 其中 A-B是shuffle依赖1 B-C是shuffle依赖2 传递的RDD参数为D,那么返回的只会是依赖2
    val (shuffleDeps, resourceProfiles) = getShuffleDependenciesAndResourceProfiles(rdd)
    logInfo(s"##### ShuffleMapStage创建 rdd=${rdd}的直系shuffle依赖对应的shuffleId为=${shuffleDeps.map(_.shuffleId)}#####")
    val resourceProfile = mergeResourceProfilesForStage(resourceProfiles)
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd, resourceProfile)
    checkBarrierStageWithRDDChainPattern(rdd, rdd.getNumPartitions)
    val numTasks = rdd.partitions.length
    // 根据shuffle依赖构建出所有的shuffleMapStage 最终返回的知识直系的父Stage
    val parents = getOrCreateParentStages(shuffleDeps, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(
      id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker,
      resourceProfile.id)

    stageIdToStage(id) = stage
    // 这里包含了shuffleId到stage的映射关系
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)

    if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo(s"Registering RDD ${rdd.id} (${rdd.getCreationSite}) as input to " +
        s"shuffle ${shuffleDep.shuffleId}")
      // 向mapOutputTracker注册shuffleId到shuffleStatus的映射
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * We don't support run a barrier stage with dynamic resource allocation enabled, it shall lead
   * to some confusing behaviors (e.g. with dynamic resource allocation enabled, it may happen that
   * we acquire some executors (but not enough to launch all the tasks in a barrier stage) and
   * later release them due to executor idle time expire, and then acquire again).
   *
   * We perform the check on job submit and fail fast if running a barrier stage with dynamic
   * resource allocation enabled.
   *
   * TODO SPARK-24942 Improve cluster resource management with jobs containing barrier stage
   */
  private def checkBarrierStageWithDynamicAllocation(rdd: RDD[_]): Unit = {
    if (rdd.isBarrier() && Utils.isDynamicAllocationEnabled(sc.getConf)) {
      throw new BarrierJobRunWithDynamicAllocationException
    }
  }

  /**
   * Check whether the barrier stage requires more slots (to be able to launch all tasks in the
   * barrier stage together) than the total number of active slots currently. Fail current check
   * if trying to submit a barrier stage that requires more slots than current total number. If
   * the check fails consecutively beyond a configured number for a job, then fail current job
   * submission.
   */
  private def checkBarrierStageWithNumSlots(rdd: RDD[_], rp: ResourceProfile): Unit = {
    if (rdd.isBarrier()) {
      val numPartitions = rdd.getNumPartitions
      val maxNumConcurrentTasks = sc.maxNumConcurrentTasks(rp)
      if (numPartitions > maxNumConcurrentTasks) {
        throw new BarrierJobSlotsNumberCheckFailed(numPartitions, maxNumConcurrentTasks)
      }
    }
  }

  private[scheduler] def mergeResourceProfilesForStage(
                                                        stageResourceProfiles: HashSet[ResourceProfile]): ResourceProfile = {
    logDebug(s"Merging stage rdd profiles: $stageResourceProfiles")
    val resourceProfile = if (stageResourceProfiles.size > 1) {
      if (shouldMergeResourceProfiles) {
        val startResourceProfile = stageResourceProfiles.head
        val mergedProfile = stageResourceProfiles.drop(1)
          .foldLeft(startResourceProfile)((a, b) => mergeResourceProfiles(a, b))
        // compared merged profile with existing ones so we don't add it over and over again
        // if the user runs the same operation multiple times
        val resProfile = sc.resourceProfileManager.getEquivalentProfile(mergedProfile)
        resProfile match {
          case Some(existingRp) => existingRp
          case None =>
            // this ResourceProfile could be different if it was merged so we have to add it to
            // our ResourceProfileManager
            sc.resourceProfileManager.addResourceProfile(mergedProfile)
            mergedProfile
        }
      } else {
        throw new IllegalArgumentException("Multiple ResourceProfiles specified in the RDDs for " +
          "this stage, either resolve the conflicting ResourceProfiles yourself or enable " +
          s"${config.RESOURCE_PROFILE_MERGE_CONFLICTS.key} and understand how Spark handles " +
          "the merging them.")
      }
    } else {
      if (stageResourceProfiles.size == 1) {
        stageResourceProfiles.head
      } else {
        sc.resourceProfileManager.defaultResourceProfile
      }
    }
    resourceProfile
  }

  // This is a basic function to merge resource profiles that takes the max
  // value of the profiles. We may want to make this more complex in the future as
  // you may want to sum some resources (like memory).
  private[scheduler] def mergeResourceProfiles(
                                                r1: ResourceProfile,
                                                r2: ResourceProfile): ResourceProfile = {
    val mergedExecKeys = r1.executorResources ++ r2.executorResources
    val mergedExecReq = mergedExecKeys.map { case (k, v) =>
      val larger = r1.executorResources.get(k).map(x =>
        if (x.amount > v.amount) x else v).getOrElse(v)
      k -> larger
    }
    val mergedTaskKeys = r1.taskResources ++ r2.taskResources
    val mergedTaskReq = mergedTaskKeys.map { case (k, v) =>
      val larger = r1.taskResources.get(k).map(x =>
        if (x.amount > v.amount) x else v).getOrElse(v)
      k -> larger
    }
    new ResourceProfile(mergedExecReq, mergedTaskReq)
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def createResultStage(
                                 rdd: RDD[_],
                                 func: (TaskContext, Iterator[_]) => _,
                                 partitions: Array[Int],
                                 jobId: Int,
                                 callSite: CallSite): ResultStage = {
    // 只获得当前rdd的`直系shuffle依赖` 比如 A - B - C --D 其中 A-B是shuffle依赖1 B-C是shuffle依赖2 传递的RDD参数为D,那么返回的只会是依赖2
    val (shuffleDeps, resourceProfiles) = getShuffleDependenciesAndResourceProfiles(rdd)
    val resourceProfile = mergeResourceProfilesForStage(resourceProfiles)
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd, resourceProfile)
    checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
    // 内部会获得所有的shuffle依赖 并生成所有的shuffleMapStage 但最终返回的只是末尾RDD的直系Stage
    val parents = getOrCreateParentStages(shuffleDeps, jobId)
    logInfo(s"=====根据直系shuffle依赖构造出父stage:${parents}=====")
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId,
      callSite, resourceProfile.id)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    logInfo(s"=====计算出了ResultStage：id=${id},stage=${stage}=====")
    stage
  }

  /**
   * Get or create the list of parent stages for the given shuffle dependencies. The new
   * Stages will be created with the provided firstJobId.
   */
  private def getOrCreateParentStages(shuffleDeps: HashSet[ShuffleDependency[_, _, _]],
                                      firstJobId: Int): List[Stage] = {
    shuffleDeps.map { shuffleDep =>
      // 最后返回的是末尾RDD对应的直系父Stage
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getMissingAncestorShuffleDependencies(
                                                     rdd: RDD[_]): ListBuffer[ShuffleDependency[_, _, _]] = {
    val ancestors = new ListBuffer[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        val (shuffleDeps, _) = getShuffleDependenciesAndResourceProfiles(toVisit)
        shuffleDeps.foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.prepend(shuffleDep)
            waitingForVisit.prepend(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
   * Returns shuffle dependencies that are immediate parents of the given RDD and the
   * ResourceProfiles associated with the RDDs for this stage.
   *
   * This function will not return more distant ancestors for shuffle dependencies. For example,
   * if C has a shuffle dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
   */
  // 只获得当前rdd的直系shuffle依赖 比如 A - B - C --D 其中 A-B是shuffle依赖1 B-C是shuffle依赖2 传递的RDD参数为D,那么返回的只会是依赖2
  private[scheduler] def getShuffleDependenciesAndResourceProfiles(
                                                                    rdd: RDD[_]): (HashSet[ShuffleDependency[_, _, _]], HashSet[ResourceProfile]) = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val resourceProfiles = new HashSet[ResourceProfile]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        Option(toVisit.getResourceProfile).foreach(resourceProfiles += _)
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }
    (parents, resourceProfiles)
  }

  /**
   * Traverses the given RDD and its ancestors within the same stage and checks whether all of the
   * RDDs satisfy a given predicate.
   */
  private def traverseParentRDDsWithinStage(rdd: RDD[_], predicate: RDD[_] => Boolean): Boolean = {
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        if (!predicate(toVisit)) {
          return false
        }
        visited += toVisit
        toVisit.dependencies.foreach {
          case _: ShuffleDependency[_, _, _] =>
          // Not within the same stage with current rdd, do nothing.
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }
    true
  }

  // 只返回直系父stage
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    // 存放的是没有分析完毕的Stage
    val missing = new HashSet[Stage]
    // 存放的是已经访问过的RDD
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    // 存放的是待访问的RDD
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += stage.rdd

    def visit(rdd: RDD[_]): Unit = {
      if (!visited(rdd)) {
        visited += rdd
        // 获取该RDD还没有缓存的分区
        // val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        val rddHasUncachedPartitions = getCacheLocsAndSizes(rdd).contains((Nil, Nil))
        // 分区不为空
        if (rddHasUncachedPartitions) {
          // 获得RDD的所有依赖
          for (dep <- rdd.dependencies) {
            dep match {
              // 如果是宽依赖(shuffle依赖)
              case shufDep: ShuffleDependency[_, _, _] =>
                // 创建ShuffleMapStage
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                // 如果当前stage还没有被计算过 那么将其加入结果集合中
                if (!mapStage.isAvailable) {
                  // 因为是宽依赖，所以不属于同一个Stage，并且创建的Stage一定是shuffleMapStage
                  missing += mapStage
                }
              // 如果是窄依赖
              case narrowDep: NarrowDependency[_] =>
                // 将依赖对应的RDD放入带访问的集合中
                waitingForVisit.prepend(narrowDep.rdd)
            }
          }
        }
      }
    }

    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    // 返回stage的所有父stage，他爷爷并不返回，只返回他爹
    missing.toList
  }

  /** Invoke `.partitions` on the given RDD and all of its ancestors */
  private def eagerlyComputePartitionsForRddAndAncestors(rdd: RDD[_]): Unit = {
    logInfo(s"=====计算当前RDD=${rdd}及其祖先的分区数======")
    // 假设DAG图是 RDD1 -> RDD2 -> RDD3 当前的RDD是RDD3
    val startTime = System.nanoTime
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    // 先加入RDD3
    waitingForVisit += rdd

    def visit(rdd: RDD[_]): Unit = {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd

        // Eagerly compute:
        // 这里依次计算的是 RDD3 RDD2 RDD1的分区
        rdd.partitions
        logInfo(s"#####当前RDD=${rdd},分区数=${rdd.partitions.length}#####")
        // 这里又遍历rdd的每个依赖 将其加入waitingForVisit队列中 就构造出整个rdd的血缘关系
        // 注意 这里是把rdd放在最前
        // 一个RDD可能有多个依赖 当前RDD3前面的依赖是RDD2
        for (dep <- rdd.dependencies) {
          // 加入到最前面
          waitingForVisit.prepend(dep.rdd)
        }
      }
    }

    while (waitingForVisit.nonEmpty) {
      // 移出的顺序是RDD3 RDD2 RDD1
      visit(waitingForVisit.remove(0))
    }
    logDebug("eagerlyComputePartitionsForRddAndAncestors for RDD %d took %f seconds"
      .format(rdd.id, (System.nanoTime - startTime) / 1e9))
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]): Unit = {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter {
          !_.jobIds.contains(jobId)
        }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }

    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
                .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int): Unit = {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
   *
   * @param rdd           target RDD to run tasks on
   * @param func          a function to run on each partition of the RDD
   * @param partitions    set of partitions to run on; some jobs may not want to compute on all
   *                      partitions of the target RDD, e.g. for operations like first()
   * @param callSite      where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties    scheduler properties to attach to this job, e.g. fair scheduler pool name
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
                       rdd: RDD[T],
                       func: (TaskContext, Iterator[T]) => U,
                       partitions: Seq[Int],
                       callSite: CallSite,
                       resultHandler: (Int, U) => Unit,
                       properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    logInfo(s"=====当前rdd:$rdd 有${rdd.partitions.length}个分区=====")
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    // 计算RDD及其祖先的分区数
    logInfo(s"-----<获取分区数>开始计算当前rdd:$rdd 及其祖先的分区数 并构造出了RDD的血缘关系-----")
    eagerlyComputePartitionsForRddAndAncestors(rdd)
    logInfo(s"-----rdd:${rdd}的分区数为${rdd.getNumPartitions}-----")
    // 生成JobId
    val jobId = nextJobId.getAndIncrement()
    logInfo(s"=====自增生成jobId=$jobId=====")
    if (partitions.isEmpty) {
      val clonedProperties = Utils.cloneProperties(properties)
      if (sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION) == null) {
        clonedProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, callSite.shortForm)
      }
      val time = clock.getTimeMillis()
      listenerBus.post(
        SparkListenerJobStart(jobId, time, Seq.empty, clonedProperties))
      listenerBus.post(
        SparkListenerJobEnd(jobId, time, JobSucceeded))
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.nonEmpty)
    // func2代表任务的执行逻辑
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter[U](this, jobId, partitions.size, resultHandler)
    // 将Job放入DAG的事件处理器中
    logInfo(s"=====将Job放入DAG的事件处理器中,即向DAG的eventProcessLoop中添加JobSubmitted事件=====")
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      Utils.cloneProperties(properties)))
    waiter
  }

  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd           target RDD to run tasks on
   * @param func          a function to run on each partition of the RDD
   * @param partitions    set of partitions to run on; some jobs may not want to compute on all
   *                      partitions of the target RDD, e.g. for operations like first()
   * @param callSite      where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties    scheduler properties to attach to this job, e.g. fair scheduler pool name
   * @note Throws `Exception` when the job fails
   */
  def runJob[T, U](
                    rdd: RDD[T],
                    func: (TaskContext, Iterator[T]) => U,
                    partitions: Seq[Int],
                    callSite: CallSite,
                    resultHandler: (Int, U) => Unit,
                    properties: Properties): Unit = {
    val start = System.nanoTime
    // 进submitJob
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)

    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd        target RDD to run tasks on
   * @param func       a function to run on each partition of the RDD
   * @param evaluator  `ApproximateEvaluator` to receive the partial results
   * @param callSite   where in the user program this job was called
   * @param timeout    maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def runApproximateJob[T, U, R](
                                  rdd: RDD[T],
                                  func: (TaskContext, Iterator[T]) => U,
                                  evaluator: ApproximateEvaluator[U, R],
                                  callSite: CallSite,
                                  timeout: Long,
                                  properties: Properties): PartialResult[R] = {
    val jobId = nextJobId.getAndIncrement()
    val clonedProperties = Utils.cloneProperties(properties)
    if (rdd.partitions.isEmpty) {
      // Return immediately if the job is running 0 tasks
      val time = clock.getTimeMillis()
      listenerBus.post(SparkListenerJobStart(jobId, time, Seq[StageInfo](), clonedProperties))
      listenerBus.post(SparkListenerJobEnd(jobId, time, JobSucceeded))
      return new PartialResult(evaluator.currentResult(), true)
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)

    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, rdd.partitions.indices.toArray, callSite, listener,
      clonedProperties))
    listener.awaitResult() // Will throw an exception if the job fails
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback   function called with the result of the job, which in this case will be a
   *                   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite   where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def submitMapStage[K, V, C](
                               dependency: ShuffleDependency[K, V, C],
                               callback: MapOutputStatistics => Unit,
                               callSite: CallSite,
                               properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter[MapOutputStatistics](
      this, jobId, 1,
      (_: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, Utils.cloneProperties(properties)))
    waiter
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int, reason: Option[String]): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId, reason))
  }

  /**
   * Cancel all jobs in the given job group ID.
   */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs(): Unit = {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      Option("as part of cancellation of all jobs")))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int, reason: Option[String]): Unit = {
    eventProcessLoop.post(StageCancelled(stageId, reason))
  }

  /**
   * Kill a given task. It will be retried.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    taskScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages(): Unit = {
    if (failedStages.nonEmpty) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /**
   * Check for waiting stages which are now eligible for resubmission.
   * Submits stages that depend on the given parent stage. Called when the parent stage completes
   * successfully.
   */
  private def submitWaitingChildStages(parent: Stage): Unit = {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String): Unit = {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_,
      Option("part of cancelled job group %s".format(groupId))))
  }

  // 开始运行单个任务的方法
  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo): Unit = {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = {
      stageIdToStage.get(task.stageId).map(_.latestInfo.attemptNumber).getOrElse(-1)
    }
    // 将任务开始的信息放入监听器中，应该是任务完成了就会上报，这里应该就是任务调度的终点了======
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
  }

  private[scheduler] def handleSpeculativeTaskSubmitted(task: Task[_]): Unit = {
    listenerBus.post(SparkListenerSpeculativeTaskSubmitted(task.stageId, task.stageAttemptId))
  }

  private[scheduler] def handleUnschedulableTaskSetAdded(
                                                          stageId: Int,
                                                          stageAttemptId: Int): Unit = {
    listenerBus.post(SparkListenerUnschedulableTaskSetAdded(stageId, stageAttemptId))
  }

  private[scheduler] def handleUnschedulableTaskSetRemoved(
                                                            stageId: Int,
                                                            stageAttemptId: Int): Unit = {
    listenerBus.post(SparkListenerUnschedulableTaskSetRemoved(stageId, stageAttemptId))
  }

  private[scheduler] def handleTaskSetFailed(
                                              taskSet: TaskSet,
                                              reason: String,
                                              exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach {
      abortStage(_, reason, exception)
    }
  }

  private[scheduler] def cleanUpAfterSchedulerStop(): Unit = {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo): Unit = {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  private[scheduler] def handleJobSubmitted(jobId: Int,
                                            finalRDD: RDD[_],
                                            func: (TaskContext, Iterator[_]) => _,
                                            partitions: Array[Int],
                                            callSite: CallSite,
                                            listener: JobListener,
                                            properties: Properties): Unit = {
    var finalStage: ResultStage = null
    try {
      logInfo(s"=====DAG接收到JobSubmitted事件:进入handleJobSubmitted=====")
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      // 根据最后传递过来的RDD，也就是action算子执行逻辑后生成的RDD，生成finalStage
      logInfo(s"#####<开始>进入createResultStage方法 开始根据传递的末尾RDD:${finalRDD}计算ResultStage#####")
      //                    RDD1 -> RDD2 -> RDD3 -> RDD4
      // shuffleDependency:     RDD1    RDD2    RDD3
      // shuffleId:               2       1       0
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
      logInfo(s"#####<结束>得到的ResultStage为${finalStage}#####")
    } catch {
      case e: BarrierJobSlotsNumberCheckFailed =>
        // If jobId doesn't exist in the map, Scala coverts its value null to 0: Int automatically.
        val numCheckFailures = barrierJobIdToNumTasksCheckFailures.compute(jobId,
          (_: Int, value: Int) => value + 1)

        logWarning(s"Barrier stage in job $jobId requires ${e.requiredConcurrentTasks} slots, " +
          s"but only ${e.maxConcurrentTasks} are available. " +
          s"Will retry up to ${maxFailureNumTasksCheck - numCheckFailures + 1} more times")

        if (numCheckFailures <= maxFailureNumTasksCheck) {
          messageScheduler.schedule(
            new Runnable {
              override def run(): Unit = eventProcessLoop.post(JobSubmitted(jobId, finalRDD, func,
                partitions, callSite, listener, properties))
            },
            timeIntervalNumTasksCheck,
            TimeUnit.SECONDS
          )
          return
        } else {
          // Job failed, clear internal data.
          barrierJobIdToNumTasksCheckFailures.remove(jobId)
          listener.jobFailed(e)
          return
        }

      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    // Job submitted, clear internal data.
    barrierJobIdToNumTasksCheckFailures.remove(jobId)

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("#####最后的stage(Final stage): " + finalStage + " (" + finalStage.name + ")#####")
    logInfo(s"#####${finalStage}的所有直系父Stage=" + finalStage.parents + "#####")
    logInfo("#####获得没有被计算的直系父Stage=" + getMissingParentStages(finalStage) + "#####")

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos,
        Utils.cloneProperties(properties)))
    // 提交Stage
    logInfo(s"#####<开始>进入submitStage(),参数为末尾RDD的ResultStage(${finalStage}),内部会调用递归方法，<先提交父Stage>#####")
    submitStage(finalStage)
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
                                                 dependency: ShuffleDependency[_, _, _],
                                                 callSite: CallSite,
                                                 listener: JobListener,
                                                 properties: Properties): Unit = {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos,
        Utils.cloneProperties(properties)))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  // 以Stage为单位进行提交，采用了递归的方法，从后往前划分出Stage，最先提交的是祖宗stage
  private def submitStage(stage: Stage): Unit = {
    // logInfo(s"=====以Stage为单位进行提交，采用了递归的方法，从后往前划分出Stage，最先提交的是祖宗stage=====")
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug(s"submitStage($stage (name=${stage.name};" +
        s"jobs=${stage.jobIds.toSeq.sorted.mkString(",")}))")
      // 如果当前stage不是等待运行/正在运行/失败的Stage，说明这个stage还没有被提交过
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        // 获得当前Stage的父Stage，注意只返回他爹，他爷爷不返回
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logInfo(s"-----计算出${stage}的所有未计算的直系父stage=${missing}-----")
        logDebug("missing: " + missing)
        // 如果不存在父Stage，递归函数的出口
        if (missing.isEmpty) {
          logInfo(s"=====当前stage=${stage},其父stage不存在或已被计算了,所以提交该stage,解析stage中的任务=====")
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          // 将stage提交给taskScheduler
          logInfo(s"#####<开始>进入submitMissingTasks(),将jobId=${jobId.get}中的stage=${stage}提交给taskScheduler#####")
          submitMissingTasks(stage, jobId.get)
          logInfo(s"#####<结束>进入submitMissingTasks(),将jobId=${jobId.get}中的stage=${stage}提交给taskScheduler#####")
          logInfo(s"################################Stage解析完毕####################################")
        } else {
          // 如果存在父Stage
          for (parent <- missing) {
            //递归提交
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /**
   * `PythonRunner` needs to know what the pyspark memory and cores settings are for the profile
   * being run. Pass them in the local properties of the task if it's set for the stage profile.
   */
  private def addPySparkConfigsToProperties(stage: Stage, properties: Properties): Unit = {
    val rp = sc.resourceProfileManager.resourceProfileFromId(stage.resourceProfileId)
    val pysparkMem = rp.getPySparkMemory
    // use the getOption on EXECUTOR_CORES.key instead of using the EXECUTOR_CORES config reader
    // because the default for this config isn't correct for standalone mode. Here we want
    // to know if it was explicitly set or not. The default profile always has it set to either
    // what user specified or default so special case it here.
    val execCores = if (rp.id == DEFAULT_RESOURCE_PROFILE_ID) {
      sc.conf.getOption(config.EXECUTOR_CORES.key)
    } else {
      val profCores = rp.getExecutorCores.map(_.toString)
      if (profCores.isEmpty) sc.conf.getOption(config.EXECUTOR_CORES.key) else profCores
    }
    pysparkMem.map(mem => properties.setProperty(PYSPARK_MEMORY_LOCAL_PROPERTY, mem.toString))
    execCores.map(cores => properties.setProperty(EXECUTOR_CORES_LOCAL_PROPERTY, cores))
  }

  /**
   * If push based shuffle is enabled, set the shuffle services to be used for the given
   * shuffle map stage for block push/merge.
   *
   * Even with dynamic resource allocation kicking in and significantly reducing the number
   * of available active executors, we would still be able to get sufficient shuffle service
   * locations for block push/merge by getting the historical locations of past executors.
   */
  private def prepareShuffleServicesForShuffleMapStage(stage: ShuffleMapStage): Unit = {
    // TODO(SPARK-32920) Handle stage reuse/retry cases separately as without finalize
    // TODO changes we cannot disable shuffle merge for the retry/reuse cases
    val mergerLocs = sc.schedulerBackend.getShufflePushMergerLocations(
      stage.shuffleDep.partitioner.numPartitions, stage.resourceProfileId)

    if (mergerLocs.nonEmpty) {
      stage.shuffleDep.setMergerLocs(mergerLocs)
      logInfo(s"Push-based shuffle enabled for $stage (${stage.name}) with" +
        s" ${stage.shuffleDep.getMergerLocs.size} merger locations")

      logDebug("List of shuffle push merger locations " +
        s"${stage.shuffleDep.getMergerLocs.map(_.host).mkString(", ")}")
    } else {
      logInfo("No available merger locations." +
        s" Push-based shuffle disabled for $stage (${stage.name})")
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
    logInfo(s"-----开始将stage:${stage}分解成taskSet-----")
    logDebug("submitMissingTasks(" + stage + ")")

    // Before find missing partition, do
    // the intermediate state clean work first.
    // The operation here can make sure for the partially completed intermediate stage,
    // `findMissingPartitions()` returns all partitions every time.
    stage match {
      case sms: ShuffleMapStage if stage.isIndeterminate && !sms.isAvailable =>
        mapOutputTracker.unregisterAllMapOutput(sms.shuffleDep.shuffleId)
      case _ =>
    }

    // Figure out the indexes of partition ids to compute.
    // 获取要计算的分区号
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
    logInfo(s"=====获取当前stage:${stage}要计算的分区号${partitionsToCompute.head} ~ ${partitionsToCompute.last}=====")
    /**
     * Custom modifications by jaken
     * 查看shuffle过程中map任务的保存的shuffleStatus
     */
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    //    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    logInfo(s"##### tracker.shuffleStatuses.keys = ${tracker.shuffleStatuses.keys} #####")
    logInfo(s"##### Array[MapStatus].head = \n${tracker.shuffleStatuses.values.map{
      status => status.mapStatuses.head
    }.mkString(" | ")
    } #####")
    val narrowAncestors = stage.rdd.getNarrowAncestors
    logInfo(s"#####getNarrowAncestors=\n${narrowAncestors},rddId=${narrowAncestors.map(_.id)} #####")
    // 初始化三个变量为false
    fromCached = false
    fromHDFS = false
    fromShuffle = false

    /**
     * Custom modifications by jaken
     * 根据窄依赖的关系 过滤出shuffleRDD和hadoopRDD
     */
    val shuffledRDDs = narrowAncestors.filter(_.isInstanceOf[ShuffledRDD[_, _, _]])
    if (shuffledRDDs.nonEmpty) fromShuffle = true
    logInfo(s"#####shuffledRDDs=${shuffledRDDs},fromShuffle=${fromShuffle} #####")

    val hadoopRDDs = narrowAncestors.filter {
      case _: HadoopRDD[_, _] | _: NewHadoopRDD[_, _] => true
      case _ => false
    }
    if (hadoopRDDs.nonEmpty) fromHDFS = true
    logInfo(s"#####hadoopRDDs=${hadoopRDDs},fromHDFS=${fromHDFS} #####")

    /*
      blockManagerMaster.getLocations(blockIds).map { bms =>
      bms.map(bm => TaskLocation(bm.host, bm.executorId))
    * */

    // 可能存在一个stage由多个shuffledRDD的结果作为输入,那么使用rdd.id最大的那个shuffledRDD作为当前stage的输入shuffledRDD
    // shuffledRDD.id越大 表明当前rdd出现的越晚 stage需要最新的shuffle作为输入 老的shuffle输入可能已经作为block保存在内存中了
    /**
     * Custom modifications by jaken
     * 找到输入的shuffleRDD 缓存的shuffleRDD 输入的hadoopRDD
     */
    val inputShuffledRDD: Option[RDD[_]] = shuffledRDDs match {
      case Nil => None
      case rdds => Some(rdds.maxBy(_.id))
    }
    val cachedShuffledRDD: Option[RDD[_]] = if (shuffledRDDs.size <= 1) None else shuffledRDDs match {
      case Nil => None
      case rdds => Some(rdds.minBy(_.id))
    }
    val inputHadoopRDD: Option[RDD[_]] = hadoopRDDs match {
      case Nil => None
      case rdds => Some(rdds.head)
    }

    /**
     * Custom modifications by jaken
     * 得到输入shuffleRDD的shuffle依赖
     */
    val inputShuffleDependency: Option[ShuffleDependency[_, _, _]] = inputShuffledRDD match {
      // 注意使用的是dependencies,来获取当前RDD的依赖 而不是getDependencies去获取所有父RDD的依赖
      case Some(rdd) => Some(rdd.asInstanceOf[ShuffledRDD[_, _, _]].dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]])
      case None => None
    }
    logInfo(s"#####inputShuffledRDD=${inputShuffledRDD},inputShuffleId=${inputShuffleDependency.map(_.shuffleId).getOrElse(-1)}#####")

    /*
    * 对于缓存的hadoopRDD 通过定义变量cacheHadoopRDDsize:mutable.HashMap[Int, mutable.IndexedSeq[Long]]来记录hadoopRDD是否已经被访问过了
    * */
    // 如果hadoopRDD第一次出现 就将其放入cacheHadoopRDDsize中 长度为stage.rdd.getNumPartitions
    /*if (hadoopRDDs.nonEmpty && !cacheHadoopRDDsize.contains(inputHadoopRDD.get.id))
      cacheHadoopRDDsize(inputHadoopRDD.get.id) = mutable.IndexedSeq.fill[Long](stage.rdd.getNumPartitions)(0)
    logInfo(s"#####cacheHadoopRDDsize=\n${cacheHadoopRDDsize.mkString("\n")}#####")*/

    // 得到blockmanager and master
    /*val blockManager = SparkEnv.get.blockManager
    val blockManagerMaster = blockManager.master
    // blockManager.blockManagerId.host=本机的host
    // logInfo(s"#####blockManager.blockManagerId.host=${blockManager.blockManagerId.host}#####")
    var hadoopRDDBlockSize: Array[(Long, Long)] = Array.empty[(Long, Long)]*/

    /**
     * Custom modifications by jaken
     * 只是打印输出看一下，没具体意义
     * 如果当前stage有hadoopRDD作为输入，根据partition找到分区对应的blockId 然后根据blockManagerMaster找到blockStatus 从而找到块的大小
     */
    /*if (hadoopRDDs.nonEmpty) {
      val hadoopRDDBlockIds: Array[BlockId] = inputHadoopRDD.get.partitions.indices.map(index => RDDBlockId(inputHadoopRDD.get.id, index)).toArray[BlockId]
      logInfo(s"#####hadoopRDDBlockIds=${hadoopRDDBlockIds.mkString(",")}#####")
      /*val hadoopRDDBlockSize = hadoopRDDBlockIds.map {
        blockId =>
          val blockStatus = blockManagerMaster.getLocationsAndStatus(blockId, blockManager.blockManagerId.host).map(_.status)
          if (blockStatus.isDefined) (blockStatus.get.memSize, blockStatus.get.diskSize)
          else (0, 0)
      }*/
      hadoopRDDBlockSize = hadoopRDDBlockIds.map { blockId =>
        val blockStatus = blockManagerMaster.getLocationsAndStatus(blockId, blockManager.blockManagerId.host).map(_.status)
        blockStatus.map(status => (status.memSize, status.diskSize)).getOrElse(0L, 0L)
      }
      logInfo(s"#####hadoopBlockSize(memSize,diskSize)=${hadoopRDDBlockSize.mkString(",")}#####")
    }*/

    /**
     * Custom modifications by jaken
     * 只是打印输出看一下，没具体意义
     * 如果存在缓存的shuffleRDD 用同样的方法获得其块的大小
     */
    /*if (cachedShuffledRDD.isDefined) {
      val shuffledRDDBlockIds: Array[BlockId] = cachedShuffledRDD.get.partitions.indices.map(index => RDDBlockId(cachedShuffledRDD.get.id, index)).toArray[BlockId]
      logInfo(s"#####缓存shuffledRDDBlockIds=${shuffledRDDBlockIds.mkString(",")}#####")
      val shuffledRDDBlockSize = shuffledRDDBlockIds.map {
        blockId =>
          val blockStatus = blockManagerMaster.getLocationsAndStatus(blockId, blockManager.blockManagerId.host).map(_.status)
          if (blockStatus.isDefined) (blockStatus.get.memSize, blockStatus.get.diskSize)
          else (0L, 0L)
      }
      logInfo(s"#####shuffledRDDBlockSize(memSize,diskSize)=${shuffledRDDBlockSize.mkString(",")}#####")
    }*/
    /*    val idToStage: Option[(Int, ShuffleMapStage)] = shuffleIdToMapStage.find(_._2 == stage)
        // 如果当前stage不是shuffleMapStage 就定义shuffleId = -1
        val shuffleId = idToStage match {
          case Some((id, _)) => {
            logInfo(s"#####当前stage是shuffleMapStage#####")
            id
          }
          case None => {
            logInfo(s"#####当前stage不是shuffleMapStage#####")
            -1
          }
        }
        logInfo(s"#####shuffleId=${shuffleId}#####")*/

    /*    var shuffleDependency: ShuffleDependency[_, _, _] = null
        if (isShuffledRDD) {
          logInfo(s"#####当前stage的初始RDD为shuffledRDD=${startRDD},末尾RDD为${stage.rdd}#####")
          // 根据shuffle依赖找到shuffleId
          shuffleDependency = startRDD.asInstanceOf[ShuffledRDD[_, _, _]].dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
          shuffleId = shuffleDependency.shuffleId
          logInfo(s"#####shuffledId=${shuffleId}#####")
        } else {
          logInfo(s"#####当前stage的初始RDD为hadoopRDD=${startRDD}#####")
        }
        // 如果当前startRDD.id 没有出现在cacheHadoopRDDsize中 就将其加入 长度为stage.rdd.getNumPartitions
        if (!cacheHadoopRDDsize.contains(startRDD.id)) cacheHadoopRDDsize(startRDD.id) = mutable.IndexedSeq.fill[Long](stage.rdd.getNumPartitions)(0)*/

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties
    addPySparkConfigsToProperties(stage, properties)

    // 将当前stage放入待运行的stage集合中
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      //是ShuffleMapStage
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
        // Only generate merger location for a given shuffle dependency once. This way, even if
        // this stage gets retried, it would still be merging blocks using the same set of
        // shuffle services.
        // 如果开启了shuffle服务选项
        if (pushBasedShuffleEnabled) {
          prepareShuffleServicesForShuffleMapStage(s)
        }
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    // 计算分区(任务)的数据位置
    /*val taskIdToLocations: Map[Int, Seq[TaskLocation]] =
      try {
        stage match {
          case s: ShuffleMapStage =>
            partitionsToCompute.map { id =>
              // 这里的rdd是stage末尾的rdd,真正计算的时候,会往前寻找父RDD,来获取父rdd的位置,对于ShuffleMapStage而言,从hdfs读取数据的是NewHadoopRDD,也可能是ShuffledRDD
              // (id, getPreferredLocs(stage.rdd, id))
              val (locs, sizes) = getPreferredLocsAndSizes(stage.rdd, id)
              logInfo(s"#####locs=${locs},sizes=${sizes} #####")
              (id, locs)
            }.toMap
          case s: ResultStage =>
            partitionsToCompute.map { id =>
              val p = s.partitions(id)
              // 这里的rdd是stage末尾的rdd,真正计算的时候,会往前寻找父RDD,来获取父rdd的位置,对于ResultStage而言,是ShuffledRDD
              // (id, getPreferredLocs(stage.rdd, p))
              val (locs, sizes) = getPreferredLocsAndSizes(stage.rdd, p)
              logInfo(s"#####locs=${locs},sizes=${sizes} #####")
              (id, locs)
            }.toMap
        }
      }
      catch {
        case NonFatal(e) =>
          stage.makeNewStageAttempt(partitionsToCompute.size)
          listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
            Utils.cloneProperties(properties)))
          abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
          runningStages -= stage
          return
      }*/
    /**
     * Custom modifications by jaken
     * 修改变量 改为taskIdToLocationsAndSizes
     */
    val taskIdToLocationsAndSizes: Map[Int, IndexedSeq[(Seq[TaskLocation], Seq[Long])]] =
      try {
        stage match {
          case s: ShuffleMapStage =>
            partitionsToCompute.map { id =>
              // 这里的rdd是stage末尾的rdd,真正计算的时候,会往前寻找父RDD,来获取父rdd的位置,对于ShuffleMapStage而言,从hdfs读取数据的是NewHadoopRDD,也可能是ShuffledRDD
              // (id, getPreferredLocs(stage.rdd, id))
              // sizes 是input的位置和结果,缓存/hadoop数据块位置/shuffle位置
              val fromHdfsCachedShuffle: mutable.IndexedSeq[(Seq[TaskLocation], Seq[Long])] = mutable.IndexedSeq.fill(3)((Nil, Nil))
              fromCached = false
              val (locs, sizes) = getPreferredLocsAndSizes(stage.rdd, id)
              // logInfo(s"#####(locs, sizes)=(${locs},${sizes})#####")
              // fromCached=true 表示当前分区被缓存了
              // 对于shuffledRDD来说,缓存的数据大小为各个上游任务对应的分区的数据之和
              // 对于hadoopRDD来说,缓存的大小就是之前的数据块的大小
              if (fromCached) {
                logInfo(s"#####当前stage的输入包括缓存RDD,下面获取shuffleRead的输入#####")
                fromHdfsCachedShuffle(1) = (locs, sizes)
                // 有缓存的情况下 另一部分输入可能来自shuffle
                if (inputShuffleDependency.isDefined) {
                  val (shuffleLocs, shuffleSizes) = tracker.getPreferredLocationsAndSizesForShuffle(inputShuffleDependency.get, id)
                  val rddPrefs = shuffleLocs.toList
                  if (rddPrefs.nonEmpty) {
                    fromHdfsCachedShuffle(2) = (rddPrefs.map(TaskLocation(_)), shuffleSizes)
                  }
                }
              }
              // 没有缓存
              else {
                if (fromHDFS) {
                  fromHdfsCachedShuffle(0) = (locs, sizes)
                }
                else if (fromShuffle) {
                  fromHdfsCachedShuffle(2) = (locs, sizes)
                }
              }
              // logInfo(s"#####fromHdfsCachedShuffle=${fromHdfsCachedShuffle.mkString("\t")} #####")
              (id, fromHdfsCachedShuffle)
            }.toMap
          case s: ResultStage =>
            partitionsToCompute.map { id =>
              val p = s.partitions(id)
              val fromHdfsCachedShuffle: mutable.IndexedSeq[(Seq[TaskLocation], Seq[Long])] = mutable.IndexedSeq.fill(3)((Nil, Nil))
              fromCached = false
              // 这里的rdd是stage末尾的rdd,真正计算的时候,会往前寻找父RDD,来获取父rdd的位置,对于ResultStage而言,是ShuffledRDD
              // (id, getPreferredLocs(stage.rdd, p))
              val (locs, sizes) = getPreferredLocsAndSizes(stage.rdd, p)
              // fromCached=true 表示当前分区被缓存了
              // 对于shuffledRDD来说,缓存的数据大小为各个上游任务对应的分区的数据之和
              // 对于hadoopRDD来说,缓存的大小就是之前的数据块的大小
              if (fromCached) {
                fromHdfsCachedShuffle(1) = (locs, sizes)
                // 有缓存的情况下 另一部分输入可能来自shuffle
                if (inputShuffleDependency.isDefined) {
                  val (shuffleLocs, shuffleSizes) = tracker.getPreferredLocationsAndSizesForShuffle(inputShuffleDependency.get, id)
                  val rddPrefs = shuffleLocs.toList
                  if (rddPrefs.nonEmpty) {
                    fromHdfsCachedShuffle(2) = (rddPrefs.map(TaskLocation(_)), shuffleSizes)
                  }
                }
              }
              // 没有缓存
              else {
                if (fromHDFS) {
                  fromHdfsCachedShuffle(0) = (locs, sizes)
                }
                else if (fromShuffle) {
                  fromHdfsCachedShuffle(2) = (locs, sizes)
                }
              }
              // logInfo(s"#####fromHdfsCachedShuffle=${fromHdfsCachedShuffle.mkString("\t")} #####")
              (id, fromHdfsCachedShuffle)
            }.toMap
        }
      }
      catch {
        case NonFatal(e) =>
          stage.makeNewStageAttempt(partitionsToCompute.size)
          listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
            Utils.cloneProperties(properties)))
          abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
          runningStages -= stage
          return
      }
    logInfo(s"=====调用getPreferredLocs(),计算出当前RDD=${stage.rdd}的<${partitionsToCompute.size}>个任务(分区)的数据位置=====\n")
    logInfo(s"#####任务的数据位置为:taskIdToLocationsAndSizes.head=${taskIdToLocationsAndSizes.head}, ... ,taskIdToLocationsAndSizes.last=${taskIdToLocationsAndSizes.last}#####")
    // 重新封装一下
    // val taskIdToLocations: Map[Int, Seq[TaskLocation]]
    // stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    // 这里注意修改,第二个参数只需要Seq[TaskLocation] 需要现将元组进行映射 只取taskLocation
    /*注意这里修改*/
    /*
    * 为了不修改原生 stage.makeNewStageAttempt 的代码逻辑 我们还是利用nativeIndex来标注 原生spark返回的位置信息是来自于
    * hdfs ? cache ? shuffle? 避免大量代码的修改
    * */
    var nativeIndex = 0
    if (fromCached) nativeIndex = 1
    else if (fromShuffle) nativeIndex = 2
    // taskIdToLocationsAndSizes: Map[Int, IndexedSeq[(Seq[TaskLocation], Seq[Long])]]
    // val t =taskIdToLocationsAndSizes.values.map(_(nativeIndex)).map(_._1).toSeq
    logInfo(s"#####nativeIndex=${nativeIndex} #####")
    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocationsAndSizes.values.map(_(nativeIndex)).map(_._1).toSeq)

    // If there are tasks to execute, record the submission time of the stage. Otherwise,
    // post the even without the submission time, which indicates that this stage was
    // skipped.
    if (partitionsToCompute.nonEmpty) {
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    }
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
      Utils.cloneProperties(properties)))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    var partitions: Array[Partition] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      var taskBinaryBytes: Array[Byte] = null
      // taskBinaryBytes and partitions are both effected by the checkpoint status. We need
      // this synchronization in case another concurrent job is checkpointing this RDD, so we get a
      // consistent view of both variables.
      RDDCheckpointData.synchronized {
        taskBinaryBytes = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }

        partitions = stage.rdd.partitions
      }

      if (taskBinaryBytes.length > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024) {
        logWarning(s"Broadcasting large task binary with size " +
          s"${Utils.bytesToString(taskBinaryBytes.length)}")
      }
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case e: Throwable =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage

        // Abort execution
        return
    }

    // 将一个stage拆分成一个taskSet
    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      // 不同类型的stage生成不同类型的tasks
      stage match {
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()
          partitionsToCompute.map { id =>
            // 得到任务的数据本地性，一个任务就对应一个分区
            val locs = taskIdToLocationsAndSizes(id)(nativeIndex)._1
            val allLocsAndSize = taskIdToLocationsAndSizes(id)
            var allSize: Long = 0
            for (i <- allLocsAndSize.indices) {
              if (i == 0 && allLocsAndSize(i)._2.nonEmpty) allSize += allLocsAndSize(i)._2.head
              else {
                if (allLocsAndSize(i)._2.nonEmpty) {
                  for (size <- allLocsAndSize(i)._2) {
                    allSize += size
                  }
                }
              }
            }
            // val sizes = taskIdToLocationsAndSizes(id)._2
            val part = partitions(id)
            stage.pendingPartitions += id
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId, stage.rdd.isBarrier(), allLocsAndSize, allSize)
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = partitions(p)
            val locs = taskIdToLocationsAndSizes(id)(nativeIndex)._1
            val allLocsAndSize = taskIdToLocationsAndSizes(id)
            // val sizes = taskIdToLocationsAndSizes(id)._2
            var allSize: Long = 0
            for (i <- allLocsAndSize.indices) {
              if (i == 0 && allLocsAndSize(i)._2.nonEmpty) allSize += allLocsAndSize(i)._2.head
              else {
                if (allLocsAndSize(i)._2.nonEmpty) {
                  for (size <- allLocsAndSize(i)._2) {
                    allSize += size
                  }
                }
              }
            }
            new ResultTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, id, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
              stage.rdd.isBarrier(), allLocsAndSize, allSize)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    logInfo(s"#####将stage=${stage}拆分成了tasks序列,前10个task为 \n${tasks.take(10).mkString("\n")}#####")
    if (tasks.nonEmpty) {
      logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) (first 15 " +
        s"tasks are for partitions ${tasks.take(15).map(_.partitionId)})")
      // 交给taskScheduler提交tasks
      logInfo(s"#################<开始>提交任务集,stageId=${stage.id},taskNum=${tasks.length}###################")
      logInfo(s"-----开始将任务封装成TaskSet,提交给taskScheduler-----")
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties,
        stage.resourceProfileId))
      logInfo(s"#################<结束>提交任务集,stageId=${stage.id},taskNum=${tasks.length}###################")
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      stage match {
        case stage: ShuffleMapStage =>
          logDebug(s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})")
          markMapStageJobsAsFinished(stage)
        case stage: ResultStage =>
          logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
      }
      // 提交剩余的stage
      submitWaitingChildStages(stage)
    }
  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)

    event.accumUpdates.foreach { updates =>
      val id = updates.id
      try {
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.setAccumulables(
            acc.toInfo(Some(updates.value), Some(acc.value)) +: event.taskInfo.accumulables)
        }
      } catch {
        case NonFatal(e) =>
          // Log the class name to make it easy to find the bad implementation
          val accumClassName = AccumulatorContext.get(id) match {
            case Some(accum) => accum.getClass.getName
            case None => "Unknown class"
          }
          logError(
            s"Failed to update accumulator $id ($accumClassName) for task ${task.partitionId}",
            e)
      }
    }
   /* logInfo(s"##### task partitionId: ${task.partitionId} ##### ")
    event.taskInfo.accumulables.foreach { accInfo =>
      val name = accInfo.name.getOrElse("Unnamed")
      val value = accInfo.value.getOrElse("No Value")
      logInfo(s"Accumulator: $name, Value: $value")
    }*/
  }

  private def postTaskEnd(event: CompletionEvent): Unit = {
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            val taskId = event.taskInfo.taskId
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    listenerBus.post(SparkListenerTaskEnd(event.task.stageId, event.task.stageAttemptId,
      Utils.getFormattedClassName(event.task), event.reason, event.taskInfo,
      new ExecutorMetrics(event.metricPeaks), taskMetrics))
  }

  /**
   * Check [[SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL]] in job properties to see if we should
   * interrupt running tasks. Returns `false` if the property value is not a boolean value
   */
  private def shouldInterruptTaskThread(job: ActiveJob): Boolean = {
    if (job.properties == null) {
      false
    } else {
      val shouldInterruptThread =
        job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
      try {
        shouldInterruptThread.toBoolean
      } catch {
        case e: IllegalArgumentException =>
          logWarning(s"${SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL} in Job ${job.jobId} " +
            s"is invalid: $shouldInterruptThread. Using 'false' instead", e)
          false
      }
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent): Unit = {
    val task = event.task
    val stageId = task.stageId

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.stageAttemptId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    if (!stageIdToStage.contains(task.stageId)) {
      // The stage may have already finished when we get this event -- e.g. maybe it was a
      // speculative task. It is important that we send the TaskEnd event in any case, so listeners
      // are properly notified and can chose to handle it. For instance, some listeners are
      // doing their own accounting and if they don't get the task end event they think
      // tasks are still running when they really aren't.
      postTaskEnd(event)

      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)

    // Make sure the task's accumulators are updated before any other processing happens, so that
    // we can post a task end event before any jobs or stages are updated. The accumulators are
    // only updated in certain cases.
    event.reason match {
      case Success =>
        task match {
          case rt: ResultTask[_, _] =>
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                // Only update the accumulator once for each result task.
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                }
              case None => // Ignore update if task's job has finished.
            }
          case _ =>
            updateAccumulators(event)
        }
      case _: ExceptionFailure | _: TaskKilled => updateAccumulators(event)
      case _ =>
    }
    postTaskEnd(event)

    event.reason match {
      case Success =>
        // An earlier attempt of a stage (which is zombie) may still have running tasks. If these
        // tasks complete, they still count and we can mark the corresponding partitions as
        // finished. Here we notify the task scheduler to skip running tasks for the same partition,
        // to save resource.
        if (task.stageAttemptId < stage.latestInfo.attemptNumber()) {
          taskScheduler.notifyPartitionCompletion(stageId, task.partitionId)
        }

        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cancelRunningIndependentStages(job, s"Job ${job.jobId} is finished.")
                    cleanupStateForJobAndIndependentStages(job)
                    try {
                      // killAllTaskAttempts will fail if a SchedulerBackend does not implement
                      // killTask.
                      logInfo(s"Job ${job.jobId} is finished. Cancelling potential speculative " +
                        "or zombie tasks for this job")
                      // ResultStage is only used by this job. It's safe to kill speculative or
                      // zombie tasks in this stage.
                      taskScheduler.killAllTaskAttempts(
                        stageId,
                        shouldInterruptTaskThread(job),
                        reason = "Stage finished")
                    } catch {
                      case e: UnsupportedOperationException =>
                        logWarning(s"Could not cancel tasks for stage $stageId", e)
                    }
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Throwable if !Utils.isFatalError(e) =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            shuffleStage.pendingPartitions -= task.partitionId
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (executorFailureEpoch.contains(execId) &&
              smt.epoch <= executorFailureEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              // The epoch of the task is acceptable (i.e., the task was launched after the most
              // recent failure we're aware of for the executor), so mark the task's output as
              // available.
              mapOutputTracker.registerMapOutput(
                shuffleStage.shuffleDep.shuffleId, smt.partitionId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // This call to increment the epoch may not be strictly necessary, but it is retained
              // for now in order to minimize the changes in behavior from an earlier version of the
              // code. This existing behavior of always incrementing the epoch following any
              // successful shuffle map stage completion may have benefits by causing unneeded
              // cached map outputs to be cleaned up earlier on executors. In the future we can
              // consider removing this call, but this will require some extra investigation.
              // See https://github.com/apache/spark/pull/17955/files#r117385673 for more details.
              mapOutputTracker.incrementEpoch()

              clearCacheLocs()

              if (!shuffleStage.isAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage.
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                markMapStageJobsAsFinished(shuffleStage)
                submitWaitingChildStages(shuffleStage)
              }
            }
        }

      case FetchFailed(bmAddress, shuffleId, _, mapIndex, _, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptNumber != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ${failedStage.latestInfo.attemptNumber}) running")
        } else {
          failedStage.failedAttemptIds.add(task.stageAttemptId)
          val shouldAbortStage =
            failedStage.failedAttemptIds.size >= maxConsecutiveStageAttempts ||
              disallowStageRetryForTest

          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, errorMessage = Some(failureMessage),
              willRetry = !shouldAbortStage)
          } else {
            logDebug(s"Received fetch failure from $task, but it's from $failedStage which is no " +
              "longer running")
          }

          if (mapStage.rdd.isBarrier()) {
            // Mark all the map as broken in the map stage, to ensure retry all the tasks on
            // resubmitted stage attempt.
            mapOutputTracker.unregisterAllMapOutput(shuffleId)
          } else if (mapIndex != -1) {
            // Mark the map whose fetch failed as broken in the map stage
            mapOutputTracker.unregisterMapOutput(shuffleId, mapIndex, bmAddress)
          }

          if (failedStage.rdd.isBarrier()) {
            failedStage match {
              case failedMapStage: ShuffleMapStage =>
                // Mark all the map as broken in the map stage, to ensure retry all the tasks on
                // resubmitted stage attempt.
                mapOutputTracker.unregisterAllMapOutput(failedMapStage.shuffleDep.shuffleId)

              case failedResultStage: ResultStage =>
                // Abort the failed result stage since we may have committed output for some
                // partitions.
                val reason = "Could not recover from a failed barrier ResultStage. Most recent " +
                  s"failure reason: $failureMessage"
                abortStage(failedResultStage, reason, None)
            }
          }

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Fetch failure will not retry stage due to testing config"
            } else {
              s"""$failedStage (${failedStage.name})
                 |has failed the maximum allowable number of
                 |times: $maxConsecutiveStageAttempts.
                 |Most recent failure reason: $failureMessage""".stripMargin.replaceAll("\n", " ")
            }
            abortStage(failedStage, abortMessage, None)
          } else { // update failedStages and make sure a ResubmitFailedStages event is enqueued
            // TODO: Cancel running tasks in the failed stage -- cf. SPARK-17064
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            failedStages += mapStage
            if (noResubmitEnqueued) {
              // If the map stage is INDETERMINATE, which means the map tasks may return
              // different result when re-try, we need to re-try all the tasks of the failed
              // stage and its succeeding stages, because the input data will be changed after the
              // map tasks are re-tried.
              // Note that, if map stage is UNORDERED, we are fine. The shuffle partitioner is
              // guaranteed to be determinate, so the input data of the reducers will not change
              // even if the map tasks are re-tried.
              if (mapStage.isIndeterminate) {
                // It's a little tricky to find all the succeeding stages of `mapStage`, because
                // each stage only know its parents not children. Here we traverse the stages from
                // the leaf nodes (the result stages of active jobs), and rollback all the stages
                // in the stage chains that connect to the `mapStage`. To speed up the stage
                // traversing, we collect the stages to rollback first. If a stage needs to
                // rollback, all its succeeding stages need to rollback to.
                val stagesToRollback = HashSet[Stage](mapStage)

                def collectStagesToRollback(stageChain: List[Stage]): Unit = {
                  if (stagesToRollback.contains(stageChain.head)) {
                    stageChain.drop(1).foreach(s => stagesToRollback += s)
                  } else {
                    stageChain.head.parents.foreach { s =>
                      collectStagesToRollback(s :: stageChain)
                    }
                  }
                }

                def generateErrorMessage(stage: Stage): String = {
                  "A shuffle map stage with indeterminate output was failed and retried. " +
                    s"However, Spark cannot rollback the $stage to re-process the input data, " +
                    "and has to fail this job. Please eliminate the indeterminacy by " +
                    "checkpointing the RDD before repartition and try again."
                }

                activeJobs.foreach(job => collectStagesToRollback(job.finalStage :: Nil))

                // The stages will be rolled back after checking
                val rollingBackStages = HashSet[Stage](mapStage)
                stagesToRollback.foreach {
                  case mapStage: ShuffleMapStage =>
                    val numMissingPartitions = mapStage.findMissingPartitions().length
                    if (numMissingPartitions < mapStage.numTasks) {
                      if (sc.getConf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
                        val reason = "A shuffle map stage with indeterminate output was failed " +
                          "and retried. However, Spark can only do this while using the new " +
                          "shuffle block fetching protocol. Please check the config " +
                          "'spark.shuffle.useOldFetchProtocol', see more detail in " +
                          "SPARK-27665 and SPARK-25341."
                        abortStage(mapStage, reason, None)
                      } else {
                        rollingBackStages += mapStage
                      }
                    }

                  case resultStage: ResultStage if resultStage.activeJob.isDefined =>
                    val numMissingPartitions = resultStage.findMissingPartitions().length
                    if (numMissingPartitions < resultStage.numTasks) {
                      // TODO: support to rollback result tasks.
                      abortStage(resultStage, generateErrorMessage(resultStage), None)
                    }

                  case _ =>
                }
                logInfo(s"The shuffle map stage $mapStage with indeterminate output was failed, " +
                  s"we will roll back and rerun below stages which include itself and all its " +
                  s"indeterminate child stages: $rollingBackStages")
              }

              // We expect one executor failure to trigger many FetchFailures in rapid succession,
              // but all of those task failures can typically be handled by a single resubmission of
              // the failed stage.  We avoid flooding the scheduler's event queue with resubmit
              // messages by checking whether a resubmit is already in the event queue for the
              // failed stage.  If there is already a resubmit enqueued for a different failed
              // stage, that event would also be sufficient to handle the current failed stage, but
              // producing a resubmit for each failed stage makes debugging and logging a little
              // simpler while not producing an overwhelming number of scheduler events.
              logInfo(
                s"Resubmitting $mapStage (${mapStage.name}) and " +
                  s"$failedStage (${failedStage.name}) due to fetch failure"
              )
              messageScheduler.schedule(
                new Runnable {
                  override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
                },
                DAGScheduler.RESUBMIT_TIMEOUT,
                TimeUnit.MILLISECONDS
              )
            }
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            val externalShuffleServiceEnabled = env.blockManager.externalShuffleServiceEnabled
            val isHostDecommissioned = taskScheduler
              .getExecutorDecommissionState(bmAddress.executorId)
              .exists(_.workerHost.isDefined)

            // Shuffle output of all executors on host `bmAddress.host` may be lost if:
            // - External shuffle service is enabled, so we assume that all shuffle data on node is
            //   bad.
            // - Host is decommissioned, thus all executors on that host will die.
            val shuffleOutputOfEntireHostLost = externalShuffleServiceEnabled ||
              isHostDecommissioned
            val hostToUnregisterOutputs = if (shuffleOutputOfEntireHostLost
              && unRegisterOutputOnHostOnFetchFailure) {
              Some(bmAddress.host)
            } else {
              // Unregister shuffle data just for one executor (we don't have any
              // reason to believe shuffle data has been lost for the entire host).
              None
            }
            removeExecutorAndUnregisterOutputs(
              execId = bmAddress.executorId,
              fileLost = true,
              hostToUnregisterOutputs = hostToUnregisterOutputs,
              maybeEpoch = Some(task.epoch),
              // shuffleFileLostEpoch is ignored when a host is decommissioned because some
              // decommissioned executors on that host might have been removed before this fetch
              // failure and might have bumped up the shuffleFileLostEpoch. We ignore that, and
              // proceed with unconditional removal of shuffle outputs from all executors on that
              // host, including from those that we still haven't confirmed as lost due to heartbeat
              // delays.
              ignoreShuffleFileLostEpoch = isHostDecommissioned)
          }
        }

      case failure: TaskFailedReason if task.isBarrier =>
        // Also handle the task failed reasons here.
        failure match {
          case Resubmitted =>
            handleResubmittedFailure(task, stage)

          case _ => // Do nothing.
        }

        // Always fail the current stage and retry all the tasks when a barrier task fail.
        val failedStage = stageIdToStage(task.stageId)
        if (failedStage.latestInfo.attemptNumber != task.stageAttemptId) {
          logInfo(s"Ignoring task failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ${failedStage.latestInfo.attemptNumber}) running")
        } else {
          logInfo(s"Marking $failedStage (${failedStage.name}) as failed due to a barrier task " +
            "failed.")
          val message = s"Stage failed because barrier task $task finished unsuccessfully.\n" +
            failure.toErrorString
          try {
            // killAllTaskAttempts will fail if a SchedulerBackend does not implement killTask.
            val reason = s"Task $task from barrier stage $failedStage (${failedStage.name}) " +
              "failed."
            val job = jobIdToActiveJob.get(failedStage.firstJobId)
            val shouldInterrupt = job.exists(j => shouldInterruptTaskThread(j))
            taskScheduler.killAllTaskAttempts(stageId, shouldInterrupt, reason)
          } catch {
            case e: UnsupportedOperationException =>
              // Cannot continue with barrier stage if failed to cancel zombie barrier tasks.
              // TODO SPARK-24877 leave the zombie tasks and ignore their completion events.
              logWarning(s"Could not kill all tasks for stage $stageId", e)
              abortStage(failedStage, "Could not kill zombie barrier tasks for stage " +
                s"$failedStage (${failedStage.name})", Some(e))
          }
          markStageAsFinished(failedStage, Some(message))

          failedStage.failedAttemptIds.add(task.stageAttemptId)
          // TODO Refactor the failure handling logic to combine similar code with that of
          // FetchFailed.
          val shouldAbortStage =
            failedStage.failedAttemptIds.size >= maxConsecutiveStageAttempts ||
              disallowStageRetryForTest

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Barrier stage will not retry stage due to testing config. Most recent failure " +
                s"reason: $message"
            } else {
              s"""$failedStage (${failedStage.name})
                 |has failed the maximum allowable number of
                 |times: $maxConsecutiveStageAttempts.
                 |Most recent failure reason: $message
               """.stripMargin.replaceAll("\n", " ")
            }
            abortStage(failedStage, abortMessage, None)
          } else {
            failedStage match {
              case failedMapStage: ShuffleMapStage =>
                // Mark all the map as broken in the map stage, to ensure retry all the tasks on
                // resubmitted stage attempt.
                mapOutputTracker.unregisterAllMapOutput(failedMapStage.shuffleDep.shuffleId)

              case failedResultStage: ResultStage =>
                // Abort the failed result stage since we may have committed output for some
                // partitions.
                val reason = "Could not recover from a failed barrier ResultStage. Most recent " +
                  s"failure reason: $message"
                abortStage(failedResultStage, reason, None)
            }
            // In case multiple task failures triggered for a single stage attempt, ensure we only
            // resubmit the failed stage once.
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            if (noResubmitEnqueued) {
              logInfo(s"Resubmitting $failedStage (${failedStage.name}) due to barrier stage " +
                "failure.")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
          }
        }

      case Resubmitted =>
        handleResubmittedFailure(task, stage)

      case _: TaskCommitDenied =>
      // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case _: ExceptionFailure | _: TaskKilled =>
      // Nothing left to do, already handled above for accumulator updates.

      case TaskResultLost =>
      // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | UnknownReason =>
      // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
      // will abort the job.
    }
  }

  private def handleResubmittedFailure(task: Task[_], stage: Stage): Unit = {
    logInfo(s"Resubmitted $task, so marking it as still running.")
    stage match {
      case sms: ShuffleMapStage =>
        sms.pendingPartitions += task.partitionId

      case _ =>
        throw new SparkException("TaskSetManagers should only send Resubmitted task " +
          "statuses for tasks in ShuffleMapStages.")
    }
  }

  private[scheduler] def markMapStageJobsAsFinished(shuffleStage: ShuffleMapStage): Unit = {
    // Mark any map-stage jobs waiting on this stage as finished
    if (shuffleStage.isAvailable && shuffleStage.mapStageJobs.nonEmpty) {
      val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
      for (job <- shuffleStage.mapStageJobs) {
        markMapStageJobAsFinished(job, stats)
      }
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using an external shuffle service), or the
   * entire Standalone worker is lost.
   */
  private[scheduler] def handleExecutorLost(
                                             execId: String,
                                             workerHost: Option[String]): Unit = {
    // if the cluster manager explicitly tells us that the entire worker was lost, then
    // we know to unregister shuffle output.  (Note that "worker" specifically refers to the process
    // from a Standalone cluster, where the shuffle service lives in the Worker.)
    val fileLost = workerHost.isDefined || !env.blockManager.externalShuffleServiceEnabled
    removeExecutorAndUnregisterOutputs(
      execId = execId,
      fileLost = fileLost,
      hostToUnregisterOutputs = workerHost,
      maybeEpoch = None)
  }

  /**
   * Handles removing an executor from the BlockManagerMaster as well as unregistering shuffle
   * outputs for the executor or optionally its host.
   *
   * @param execId                  executor to be removed
   * @param fileLost                If true, indicates that we assume we've lost all shuffle blocks associated
   *                                with the executor; this happens if the executor serves its own blocks (i.e., we're not
   *                                using an external shuffle service), the entire Standalone worker is lost, or a FetchFailed
   *                                occurred (in which case we presume all shuffle data related to this executor to be lost).
   * @param hostToUnregisterOutputs (optional) executor host if we're unregistering all the
   *                                outputs on the host
   * @param maybeEpoch              (optional) the epoch during which the failure was caught (this prevents
   *                                reprocessing for follow-on fetch failures)
   */
  private def removeExecutorAndUnregisterOutputs(
                                                  execId: String,
                                                  fileLost: Boolean,
                                                  hostToUnregisterOutputs: Option[String],
                                                  maybeEpoch: Option[Long] = None,
                                                  ignoreShuffleFileLostEpoch: Boolean = false): Unit = {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    logDebug(s"Considering removal of executor $execId; " +
      s"fileLost: $fileLost, currentEpoch: $currentEpoch")
    if (!executorFailureEpoch.contains(execId) || executorFailureEpoch(execId) < currentEpoch) {
      executorFailureEpoch(execId) = currentEpoch
      logInfo(s"Executor lost: $execId (epoch $currentEpoch)")
      if (pushBasedShuffleEnabled) {
        // Remove fetchFailed host in the shuffle push merger list for push based shuffle
        hostToUnregisterOutputs.foreach(
          host => blockManagerMaster.removeShufflePushMergerLocation(host))
      }
      blockManagerMaster.removeExecutor(execId)
      clearCacheLocs()
    }
    if (fileLost) {
      val remove = if (ignoreShuffleFileLostEpoch) {
        true
      } else if (!shuffleFileLostEpoch.contains(execId) ||
        shuffleFileLostEpoch(execId) < currentEpoch) {
        shuffleFileLostEpoch(execId) = currentEpoch
        true
      } else {
        false
      }
      if (remove) {
        hostToUnregisterOutputs match {
          case Some(host) =>
            logInfo(s"Shuffle files lost for host: $host (epoch $currentEpoch)")
            mapOutputTracker.removeOutputsOnHost(host)
          case None =>
            logInfo(s"Shuffle files lost for executor: $execId (epoch $currentEpoch)")
            mapOutputTracker.removeOutputsOnExecutor(execId)
        }
      }
    }
  }

  /**
   * Responds to a worker being removed. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use workerRemoved() to post a loss event from outside.
   *
   * We will assume that we've lost all shuffle blocks associated with the host if a worker is
   * removed, so we will remove them all from MapStatus.
   *
   * @param workerId identifier of the worker that is removed.
   * @param host     host of the worker that is removed.
   * @param message  the reason why the worker is removed.
   */
  private[scheduler] def handleWorkerRemoved(
                                              workerId: String,
                                              host: String,
                                              message: String): Unit = {
    logInfo("Shuffle files lost for worker %s on host %s".format(workerId, host))
    mapOutputTracker.removeOutputsOnHost(host)
    clearCacheLocs()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String): Unit = {
    // remove from executorFailureEpoch(execId) ?
    if (executorFailureEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      executorFailureEpoch -= execId
    }
    shuffleFileLostEpoch -= execId
  }

  private[scheduler] def handleStageCancellation(stageId: Int, reason: Option[String]): Unit = {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          val reasonStr = reason match {
            case Some(originalReason) =>
              s"because $originalReason"
            case None =>
              s"because Stage $stageId was cancelled"
          }
          handleJobCancellation(jobId, Option(reasonStr))
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: Option[String]): Unit = {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason.getOrElse("")))
    }
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(
                                   stage: Stage,
                                   errorMessage: Option[String] = None,
                                   willRetry: Boolean = false): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }

    if (!willRetry) {
      outputCommitCoordinator.stageEnd(stage.id)
    }
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
                                     failedStage: Stage,
                                     reason: String,
                                     exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Cancel all independent, running stages that are only used by this job. */
  private def cancelRunningIndependentStages(job: ActiveJob, reason: String): Boolean = {
    var ableToCancelStages = true
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError(s"No stages registered for job ${job.jobId}")
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This stage is only used by the job, so finish the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptTaskThread(job))
              markStageAsFinished(stage, Some(reason))
            } catch {
              case e: UnsupportedOperationException =>
                logWarning(s"Could not cancel tasks for stage $stageId", e)
                ableToCancelStages = false
            }
          }
        }
      }
    }
    ableToCancelStages
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
                                           job: ActiveJob,
                                           failureReason: String,
                                           exception: Option[Throwable] = None): Unit = {
    if (cancelRunningIndependentStages(job, failureReason)) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      val error = new SparkException(failureReason, exception.orNull)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += stage.rdd

    def visit(rdd: RDD[_]): Unit = {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.prepend(mapStage.rdd)
              } // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.prepend(narrowDep.rdd)
          }
        }
      }
    }

    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd       whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Custom modifications by jaken
   * partitionSize
   */
  private[spark]
  def getPreferredLocsAndSizes(rdd: RDD[_], partition: Int): (Seq[TaskLocation], Seq[Long]) = {
    getPreferredLocsAndSizesInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
                                        rdd: RDD[_],
                                        partition: Int,
                                        visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // logInfo(s"=====递归获取任务运行的位置=====")
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    // 如果当前rdd已经被访问过，就不需要再次访问了，返回Nil
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      logInfo(s"=====<获取partition:${partition}的偏好位置>当前rdd:${rdd}中的partition:${partition}已经被访问过了,返回Nil=====")
      return Nil
    }
    // If the partition is cached, return the cache locations
    // 如果rdd被缓存过，就返回缓存的位置信息
    // logInfo(s"#####<getPreferredLocsInternal> rdd=${rdd}, partition=${partition}, dependencies=${rdd.dependencies} ,visited=${visited}#####")
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      logInfo(s"=====<获取partition:${partition}的偏好位置>当前rdd(${rdd})的partition(${partition})已被缓存,其位置为${cached}=====")
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    // 如果有位置偏好 那就选择偏好的位置
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }
    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    // 如果RDD有窄依赖 那么选择有位置偏好的最老的祖先RDD的第一个分区
    // 在理想情况下应当计算出所有的传输大小然后选择代价最低的，但这是未来要做的事情
    // rdd.dependencies只会保存父RDD的依赖,其祖先的依赖并不在其中,使用for原因是当前RDD可能有多个父RDD
    rdd.dependencies.foreach {
      // 遍历rdd的依赖,如果是窄依赖 就继续往前找,找到最老的RDD
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          // 递归计算rdd的偏好位置
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }
      case _ =>
    }
    Nil
  }

  private def getPreferredLocsAndSizesInternal(
                                                rdd: RDD[_],
                                                partition: Int,
                                                visited: HashSet[(RDD[_], Int)]): (Seq[TaskLocation], Seq[Long]) = {
    // logInfo(s"=====递归获取任务运行的位置=====")
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    // 如果当前rdd已经被访问过，就不需要再次访问了，返回Nil
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      // logInfo(s"=====<获取partition:${partition}的偏好位置>当前rdd:${rdd}中的partition:${partition}已经被访问过了,返回Nil=====")
      return (Nil, Nil)
    }
    // If the partition is cached, return the cache locations
    // 如果rdd被缓存过，就返回缓存的位置信息
    // logInfo(s"#####<getPreferredLocsInternal> rdd=${rdd}, partition=${partition}, dependencies=${rdd.dependencies} ,visited=${visited}#####")
    /*val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      logInfo(s"=====<获取partition:${partition}的偏好位置>当前rdd(${rdd})的partition(${partition})已被缓存,其位置为${cached}=====")
      return (cached,Nil)
    }*/
    val (cachedLocs, cachedSize) = getCacheLocsAndSizes(rdd)(partition)
    if (cachedLocs.nonEmpty) {
      // logInfo(s"=====<获取partition:${partition}的偏好位置>当前rdd(${rdd})的partition(${partition})已被缓存,其位置为${cachedLocs},分区大小为${cachedSize}=====")
      fromCached = true
      return (cachedLocs, cachedSize)
    }
    // val cachedLocsAndSizes = getCacheLocsAndSizes(rdd)(partition)
    //    logInfo(s"all cacheLocsAndSizes=${cacheLocsAndSizes}")
    //    logInfo(s"cachedLocsAndSizes=${cachedLocsAndSizes._1}\n${cachedLocsAndSizes._2}")
    //    if (cachedLocsAndSizes._1.nonEmpty) {
    //      logInfo(s"=====<获取partition:${partition}的偏好位置>当前rdd(${rdd})的partition(${partition})已被缓存,其位置为${cachedLocsAndSizes._1},数据块大小为${cachedLocsAndSizes._2}=====")
    //      return cachedLocsAndSizes
    //    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    // 如果有位置偏好 那就选择偏好的位置
    // TODO 当shuffle的每个分片很小 也就是数据本地性等级为no_pref时 locations为null
    val (locations, sizes) = rdd.preferredLocationsAndSizes(rdd.partitions(partition))
    val rddPrefs = locations.toList
    if (rddPrefs.nonEmpty) {
      return (rddPrefs.map(TaskLocation(_)), sizes)
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    // 如果RDD有窄依赖 那么选择第一个(这就是为什么递归算法中找到了就直接使用return 而不是寻找最老的RDD)有位置偏好的老的祖先RDD的第一个分区
    // 在理想情况下应当计算出所有的传输大小然后选择代价最低的，但这是未来要做的事情
    // rdd.dependencies只会保存父RDD的依赖,其祖先的依赖并不在其中,使用for原因是当前RDD可能有多个父RDD
    rdd.dependencies.foreach {
      // 遍历rdd的依赖,如果是窄依赖 就继续往前找,找到最老的RDD
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          // 递归计算rdd的偏好位置
          val (locs, sizes) = getPreferredLocsAndSizesInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return (locs, sizes)
          }
        }

      case _ =>
    }
    (Nil, Nil)
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop(): Unit = {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
// 继承了EventLoop
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      // 进
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerHost = reason match {
        case ExecutorProcessLost(_, workerHost, _) => workerHost
        case ExecutorDecommission(workerHost) => workerHost
        case _ => None
      }
      dagScheduler.handleExecutorLost(execId, workerHost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    // 开始运行单个任务的事件
    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task)

    case UnschedulableTaskSetAdded(stageId, stageAttemptId) =>
      dagScheduler.handleUnschedulableTaskSetAdded(stageId, stageAttemptId)

    case UnschedulableTaskSetRemoved(stageId, stageAttemptId) =>
      dagScheduler.handleUnschedulableTaskSetRemoved(stageId, stageAttemptId)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)
      // 处理已经完成的任务 更新任务和executor的metrics信息等
    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200

  // Number of consecutive stage attempts allowed before a stage is aborted
  val DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS = 4
}
