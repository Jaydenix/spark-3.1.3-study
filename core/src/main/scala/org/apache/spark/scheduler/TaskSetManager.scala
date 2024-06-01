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
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.scheduler.TaskLocality._
import org.apache.spark.util.{AccumulatorV2, Clock, LongAccumulator, SystemClock, Utils}
import org.apache.spark.util.collection.MedianHeap

import scala.collection.mutable
import scala.util.Random

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and handleSuccessfulTask/handleFailedTask, which tells it that one of its tasks changed state
 * (e.g. finished/failed).
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
                                     val taskSet: TaskSet,
                                     val maxTaskFailures: Int,
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
  // 可以借助这个属性 来找到partition到index的关系
  private[scheduler] val partitionToIndex = tasks.zipWithIndex
    .map { case (t, idx) => t.partitionId -> idx }.toMap
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)

  val speculationEnabled = conf.get(SPECULATION_ENABLED)
  // Quantile of tasks at which to start speculation
  val speculationQuantile = conf.get(SPECULATION_QUANTILE)
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
  // 标记了任务是否成功完成的字段，如果任务重复了很多次也没有完成(可能来源数据丢失了)，也会为true
  val successful = new Array[Boolean](numTasks)
  private val numFailures = new Array[Int](numTasks)

  // Add the tid of task into this HashSet when the task is killed by other attempt tasks.
  // This happened while we set the `spark.speculation` to true. The task killed by others
  // should not resubmit while executor lost.
  private val killedByOtherAttempt = new HashSet[Long]

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  private[scheduler] var tasksSuccessful = 0

  val weight = 1
  val minShare = 0
  var priority = taskSet.priority
  var stageId = taskSet.stageId
  val name = "TaskSet_" + taskSet.id
  var parent: Pool = null
  private var totalResultSize = 0L
  private var calculatedTasks = 0

  private[scheduler] val taskSetExcludelistHelperOpt: Option[TaskSetExcludelist] = {
    healthTracker.map { _ =>
      new TaskSetExcludelist(sched.sc.listenerBus, conf, stageId, taskSet.stageAttemptId, clock)
    }
  }

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
  // "zombie"状态指的是任务集管理器处于一种已完成但仍然需要追踪和记录运行任务的状态,通常在以下情况之一进入zombie状态：
  // 1.所有任务尝试都已经成功完成。
  // 2.任务集被中止（例如，由于被手动终止）。
  private[scheduler] var isZombie = false

  // Whether the taskSet run tasks from a barrier stage. Spark must launch all the tasks at the
  // same time for a barrier stage.
  private[scheduler] def isBarrier = taskSet.tasks.nonEmpty && taskSet.tasks(0).isBarrier

  // Store tasks waiting to be scheduled by locality preferences
  logInfo(s"=====初始化pendingTasks栈=====")
  private[scheduler] val pendingTasks = new PendingTasksByLocality()

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet. The HashSet here ensures that we do not add
  // duplicate speculatable tasks.
  private[scheduler] val speculatableTasks = new HashSet[Int]

  // Store speculatable tasks by locality preferences
  private[scheduler] val pendingSpeculatableTasks = new PendingTasksByLocality()

  // Task index, start and finish time for each task attempt (indexed by task ID)
  // (taskIndex,TaskInfo)  TaskInfo中包含了任务的完成时间
  private[scheduler] val taskInfos = new HashMap[Long, TaskInfo]

  // Use a MedianHeap to record durations of successful tasks so we know when to launch
  // speculative tasks. This is only used when speculation is enabled, to avoid the overhead
  // of inserting into the heap when the heap won't be used.
  // 创建了一个中位数堆的数据结构 能够很快地找到运行完的任务执行时间的中位数
  val successfulTaskDurations = new MedianHeap()

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
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
  /**
   * Custom modifications by jaken
   * 使用map来记录各个任务队列中的 *真实的未被执行的任务数* ,key为对应的forXXX中的key,value为队列中的 *真实的未被执行的任务数*
   */
  private val unscheduledTaskCount = new HashMap[String, Int]

  initUnscheduledTaskCount()

  private def initUnscheduledTaskCount(): Unit = {
    for (elem <- pendingTasks.forExecutor) {
      unscheduledTaskCount.getOrElseUpdate(elem._1, elem._2.length)
    }
    for (elem <- pendingTasks.forHost) {
      unscheduledTaskCount.getOrElseUpdate(elem._1, elem._2.length)
    }
    for (elem <- pendingTasks.forRack) {
      unscheduledTaskCount.getOrElseUpdate(elem._1, elem._2.length)
    }
  }

  logInfo(s"#####初始化unscheduledTaskCount=${unscheduledTaskCount} #####")

  private def addPendingTasks(): Unit = {
    val (_, duration) = Utils.timeTakenMs {
      // 按照任务索引倒序放到阻塞任务队列(forExecutor forHost ... )中
      for (i <- (0 until numTasks).reverse) {
        // 将任务一个一个根据数据所在的位置 放入不同的HashMap中
        // forExecutor forHost forRack ...
        addPendingTask(i, resolveRacks = false)
      }
      // 这里分析每个主机在不在同一个机架上
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
   * Custom modifications by jaken
   * sort queue by task.allSize
   */
  // ascSortPendingTasks()

  private def ascSortPendingTasks(speculatable: Boolean = false): Unit = {
    logInfo(s"============升序排序前============")
    logInfo(s"前50个pendingTasks.forExecutor=\n${pendingTasks.forExecutor.take(50).mkString("\n")}\n" +
      s"前50个pendingTasks.forHost=\n${pendingTasks.forHost.take(50).mkString("\n")}\n" +
      s"前50个pendingTasks.noPrefs=\n${pendingTasks.noPrefs.take(50)}\n" +
      s"前50个pendingTasks.forRack=\n${pendingTasks.forRack.take(50).mkString("\n")}\n" +
      s"前50个pendingTasks.all=\n${pendingTasks.all.take(50)}\n")
    if (isZombie) return
    val pendingTaskSetToAddTo = if (speculatable) pendingSpeculatableTasks else pendingTasks
    pendingTaskSetToAddTo.forExecutor.foreach {
      case (executorName, taskIds) => {
        // 加个负号表示降序
        pendingTaskSetToAddTo.forExecutor(executorName) = taskIds.sortBy(index => tasks(index).taskSize)
      }
    }
    pendingTaskSetToAddTo.forHost.foreach {
      case (hostName, taskIds) => {
        // 加个负号表示降序
        pendingTaskSetToAddTo.forHost(hostName) = taskIds.sortBy(index => tasks(index).taskSize)
      }
    }
    pendingTaskSetToAddTo.forRack.foreach {
      case (rackName, taskIds) => {
        // 加个负号表示降序
        pendingTaskSetToAddTo.forRack(rackName) = taskIds.sortBy(index => tasks(index).taskSize)
      }
    }
    pendingTaskSetToAddTo.noPrefs = pendingTaskSetToAddTo.noPrefs.sortBy(index => tasks(index).taskSize)
    pendingTaskSetToAddTo.all = pendingTaskSetToAddTo.all.sortBy(index => tasks(index).taskSize)

    logInfo(s"============升序排序后============")
    logInfo(s"前50个pendingTasks.forExecutor=\n${pendingTasks.forExecutor.take(50).mkString("\n")}\n" +
      s"前50个pendingTasks.forHost=\n${pendingTasks.forHost.take(50).mkString("\n")}\n" +
      s"前50个pendingTasks.noPrefs=\n${pendingTasks.noPrefs.take(50)}\n" +
      s"前50个pendingTasks.forRack=\n${pendingTasks.forRack.take(50).mkString("\n")}\n" +
      s"前50个pendingTasks.all=\n${pendingTasks.all.take(50)}\n")
  }

  /**
   * Track the set of locality levels which are valid given the tasks locality preferences and
   * the set of currently available executors.  This is updated as executors are added and removed.
   * This allows a performance optimization, of skipping levels that aren't relevant (e.g., skip
   * PROCESS_LOCAL if no tasks could be run PROCESS_LOCAL for the current set of executors).
   */
  // 计算返回所有任务的数据本地性，顺序为PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
  // 总是最大的数据本地性等级排到最前面
  // 例：假设noPrefs的Hashmap中没有元素 myLocalityLevels = [PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY]
  private[scheduler] var myLocalityLevels = computeValidLocalityLevels()
  logInfo(s"======#####初始化myLocalityLevels = ${myLocalityLevels.mkString("Array(", ", ", ")")}#####======")

  // Time to wait at each level
  private[scheduler] var localityWaits = myLocalityLevels.map(getLocalityWait)

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last reset the locality wait timer, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task when resetting the timer
  // 决定任务调度延后的变量
  private val legacyLocalityWaitReset = conf.get(LEGACY_LOCALITY_WAIT_RESET)
  private var currentLocalityIndex = 0 // Index of our current locality level in validLocalityLevels
  private var lastLocalityWaitResetTime = clock.getTimeMillis() // Time we last reset locality wait

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  private[scheduler] var emittedTaskSizeWarning = false

  /** Add a task to all the pending-task lists that it should be on. */
  // 第 index个任务应当放在哪个任务队列中?
  private[spark] def addPendingTask(
                                     index: Int,
                                     resolveRacks: Boolean = true,
                                     speculatable: Boolean = false): Unit = {
    // A zombie TaskSetManager may reach here while handling failed task.
    if (isZombie) return
    val pendingTaskSetToAddTo = if (speculatable) pendingSpeculatableTasks else pendingTasks
    // 一个任务应当运行的位置信息，分为以下情况
    // 1. executor_[hostname]_[executorid]
    // 2. [hostname]
    // 3. hdfs_cache_[hostname]
    // 遍历当前任务的数据所在地
    for (loc <- tasks(index).preferredLocations) {
      // 如果数据是缓存在executor中的
      loc match {
        case e: ExecutorCacheTaskLocation =>
          // 将任务放到forExecutor这个HashMap中，形式为[[executor1,[task1,task2,task3]],[executor2,[task1,task2,task3]] ... ]
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
      // 将任务放到数据所在的主机的队列中
      pendingTaskSetToAddTo.forHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index

      // 如果能够解析机架信息 就存放到机架队列里面
      if (resolveRacks) {
        sched.getRackForHost(loc.host).foreach { rack =>
          pendingTaskSetToAddTo.forRack.getOrElseUpdate(rack, new ArrayBuffer) += index
        }
      }
    }

    // 当任务的preferredLocations集合为Nil,就代表任务没有数据本地性偏好,
    // 在计算任务数据本地性等级的时候,如果任务数据非常分散,单个数据/数据总量都<0.2 则preferredLocations=Nil
    if (tasks(index).preferredLocations == Nil) {
      pendingTaskSetToAddTo.noPrefs += index
    }
    // 将所有的任务都加到all这个集合中
    pendingTaskSetToAddTo.all += index
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   */
  // 从后往前取，取一个任务列表中的任务 并返回任务id
  private def dequeueTaskFromList(
                                   execId: String,
                                   host: String,
                                   list: ArrayBuffer[Int],
                                   speculative: Boolean = false): Option[Int] = {
    // logInfo(s"=====进入dequeueTaskFromList方法=====")
    var indexOffset = list.size
    // 从后往前取任务
    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset)
      // logInfo(s"#####index(partitionId)=${index}#####")
      // 当前executor/host可用
      if (!isTaskExcludededOnExecOrNode(index, execId, host) &&
        // 取反 speculative开启 并且 hasAttemptOnHost会判断任务是否在host上运行过 如果运行了就会从任务队列中删除
        !(speculative && hasAttemptOnHost(index, host))) {
        // This should almost always be list.trimEnd(1) to remove tail
        // 将任务取出来
        logInfo(s"=====dequeue调用 dequeueTaskFromList 在真正取出任务的时候 移除任务${index} 最后移除的是返回的任务=====")
        list.remove(indexOffset)
        // Speculatable task should only be launched when at most one copy of the
        // original task is running
        if (!successful(index)) {
          if (copiesRunning(index) == 0) {
            /**
             * Custom modifications by jaken
             * 如果慢节点需要跳过此次调度 就把其从队列中取出来的任务放回去 防止当进入ANY等级时 再次将未被调度的任务取出来 导致任务一直未被执行
             */
            if (skipResourceOffer) {
              logInfo(s"=====当前慢节点需要跳过此次调度,将任务${index}放回去,之后遍历其数据所在地,统一删除=====")
              list += index
            }
            return Some(index)
          } else if (speculative && copiesRunning(index) == 1) {
            /**
             * Custom modifications by jaken
             * 如果慢节点需要跳过此次调度 就把其从队列中取出来的任务方法回去 防止当进入ANY等级时 再次将未被调度的任务取出来 导致任务一直未被执行
             */
            if (skipResourceOffer) list += index
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
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def dequeueTask(
                           execId: String,
                           host: String,
                           maxLocality: TaskLocality.Value): Option[(Int, TaskLocality.Value, Boolean)] = {
    // Tries to schedule a regular task first; if it returns None, then schedules
    // a speculative task
    /**
     * 当Spark发现某个任务执行速度较慢时，它会启动一个额外的“推测性任务”在另一个可用的节点上执行相同的任务。
     * 这两个任务将同时运行，但只有一个任务能够完成，而另一个任务将被终止。一旦有一个任务完成，
     * Spark会取消执行另一个任务，并使用已完成任务的结果。
     * */
    dequeueTaskHelper(execId, host, maxLocality, false).orElse(
      dequeueTaskHelper(execId, host, maxLocality, true))
  }

  /**
   * Custom modifications by jaken
   * 将任务的所在位置 根据数据本地性等级 转化成队列中对应的key
   */
  private def parseLocationToString(taskLocation: TaskLocation, taskLocality: TaskLocality): String = {
    taskLocality match {
      case PROCESS_LOCAL =>
        val Array(_, executorId) = taskLocation.toString.stripPrefix("executor_").split("_", 2)
        executorId
      case NODE_LOCAL => taskLocation.toString
    }
  }

  /**
   * Custom modifications by jaken
   * 将任务迁移到与其对应的快节点中
   * fastHostList:当前节点对应的快节点
   * currTime:当前系统时间
   * queueMap：当前等级对应的任务队列
   * index： 任务的index 也就是partitionId
   * task：要迁移的任务
   * host：慢主机
   * */
  private def migrateTaskToFastNode(fastHostList: Seq[String], currTime: Long, queueMap: HashMap[String, ArrayBuffer[Int]],
                                    index: Int, task: Task[_], host: String): Boolean = {
    for (fastHost <- fastHostList) {
      // 快节点也不堵塞
      if (currTime > hostBlockTime(fastHost)) {
        logInfo(s"=====将任务：partitionId=${index}从慢节点(${host})加入到快节点队列(${fastHost})中=====")
        // 可能出现快节点队列中key为空 已经从queueMap中删除的情况
        if (!queueMap.contains(fastHost)) queueMap.put(fastHost, ArrayBuffer.empty)
        queueMap(fastHost) += index
        // taskSize单位B bandwidth单位MB taskSize右移20位表示除以2^20转换成MB
        val blockTime = ((task.taskSize >> 20) * 1000 / bandwidth) + currTime
        logInfo(s"#####当前系统时间${currTime}ms,任务迁移之后的阻塞截止时间: blockTime = ${blockTime}ms#####")
        hostBlockTime(fastHost) = blockTime
        hostBlockTime(host) = blockTime
        // 标注当前任务已经迁移
        movedTaskIds(index) = true
        return true
      } else {
        logInfo(s"=====快节点(${fastHost})网络阻塞,尝试下一节点或者结束=====")
      }
    }
    false
  }

  protected def dequeueTaskHelper(
                                   execId: String,
                                   host: String,
                                   maxLocality: TaskLocality.Value,
                                   speculative: Boolean): Option[(Int, TaskLocality.Value, Boolean)] = {
    if (speculative && speculatableTasks.isEmpty) {
      return None
    }
    if (speculative) {
      logInfo(s"#####触发推测执行,speculatableTasks=${speculatableTasks},maxLocality=${maxLocality}\n " +
        s"前20个pendingSpeculatableTasks.forExecutor=${pendingSpeculatableTasks.forExecutor.take(20)}\n" +
        s"前20个pendingSpeculatableTasks.forHost=${pendingSpeculatableTasks.forHost.take(20)}\n" +
        s"前20个pendingSpeculatableTasks.noPrefs=${pendingSpeculatableTasks.noPrefs.take(20)}\n" +
        s"前20个pendingSpeculatableTasks.forRack=${pendingSpeculatableTasks.forRack.take(20)}\n" +
        s"前20个pendingSpeculatableTasks.all=${pendingSpeculatableTasks.all.take(20)}\n" +
        "#####)")
    }
    val pendingTaskSetToUse = if (speculative) pendingSpeculatableTasks else pendingTasks

    // list是分配到executor/host/rack上的任务id数组
    def dequeue(list: ArrayBuffer[Int]): Option[Int] = {
      // 从list中取出最后一个未被执行的任务
      val task = dequeueTaskFromList(execId, host, list, speculative)
      // val a = for (elem <- tasks(task.get).preferredLocations) {elem.host}
      /**
       * Custom modifications by jaken
       * 统计任务队列中 未被执行的任务数目,因为spark使用的是懒移除,
       * 也就是只有当对应Key的任务队列被使用时,才会移除已经执行完毕或者正在执行的任务
       */
      /*      if(task.isDefined) {
              logInfo(s"#####被移除任务taskId=${partitionToIndex(task.get)},partitionId=${tasks(partitionToIndex(task.get))}," +
                s"preferredLocations=${tasks(partitionToIndex(task.get)).preferredLocations}#####")
            }*/
      if (speculative && task.isDefined) {
        speculatableTasks -= task.get
      }
      task
    }

    // 按照数据本地性等级取任务======================================================================
    // forExecutor-> forHost-> noPrefs -> forRack-> all 在NODE_LOCAL之后寻找noPref任务，以尽量减少跨架流量。
    // 这里返回的index是出队列的任务id

    // 先看能不能从forExecutor中取出任务
    dequeue(pendingTaskSetToUse.forExecutor.getOrElse(execId, ArrayBuffer()))
      // 实际上就只有一个元素
      .foreach {
        index =>
          // 如果skipResourceOffer==ture 表明本次任务不会调度 不更新unscheduledTaskCount的内容
          if (!skipResourceOffer) {
            for (elem <- tasks(partitionToIndex(index)).preferredLocations) {
              // TaskLocation e.g. executor_jk01_7
              // val Array(_, executorId) = elem.toString.stripPrefix("executor_").split("_", 2)
              unscheduledTaskCount(parseLocationToString(elem, PROCESS_LOCAL)) -= 1
              // process_local 也删一个
              unscheduledTaskCount(elem.host) -= 1
              /*            // 如果数据本地性等级是PROCESS_LOCAL 除了要删除PROCESS_LOCAL队列的任务之外 host中的也要删掉 暂不考虑rack
                          unscheduledTaskCount(parseLocationToString(elem, NODE_LOCAL)) -= 1*/
            }
          }

          return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
      }

    // 若forExecutor中没有任务了，maxLocality 本地性等级 >= TaskLocality.NODE_LOCAL，就去forHost中取
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      dequeue(pendingTaskSetToUse.forHost.getOrElse(host, ArrayBuffer())).foreach { index =>
        // 如果skipResourceOffer==ture 表明本次任务不会调度 不更新unscheduledTaskCount的内容
        if (!skipResourceOffer) {
          for (elem <- tasks(partitionToIndex(index)).preferredLocations) {
            unscheduledTaskCount(elem.host) -= 1
          }
        }
        return Some((index, TaskLocality.NODE_LOCAL, speculative))
      }
    }

    // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
    // maxLocality 本地性等级 >= TaskLocality.NO_PREF
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // 从NO_PREF队列中取出的任务等级是PROCESS_LOCAL
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
    logInfo(s"#####没有从任何队列中取出任务#####")
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
   * Custom modifications by jaken
   * hostScheduledTaskCount: 主机上被调度的任务数
   * hostBlockTime: 主机网络堵塞的预计时间
   * bandwidth: 主机带宽 单位Mb
   * hostPerformanceDiffer: 用来标识 对于当前调度的节点来说 哪些是快节点 以便将任务放置在这些节点上 (fastHost,performanceDiff)
   * executorScheduledTaskCount: executor上被调度的任务数
   * executorToDurationWithoutFetch: executor->(运行成功的Task的执行时间,不包括抓取数据 , 任务的数据量)
   * scheduledTaskIds: 被调度的任务,用来避免重复将任务放入上述集合中
   * successfulTaskIds： 已经完成的任务,用来避免重复将任务放入上述集合中
   * movedTaskIds: 迁移成功的任务,避免重复将相同任务迁移到不同节点上,当快节点多的时候 这样做是非常有必要的
   */
  private val hostScheduledTaskCount: mutable.HashMap[String, Int] = mutable.HashMap.empty
  // 以ms为单位 和系统时间作比较
  private val hostBlockTime: mutable.HashMap[String, Long] = mutable.HashMap.empty
  //pendingTasks.forHost.keySet 中有完整的host 但是如果某个节点上没数据 也没有host 所以最好还是不在这里面初始化
  /*  // 在这里初始化是行不通的 因为可能都开始任务调度了 但是hostToExecutors映射关系都还没创建好
    private val hostBlock = sched.hostToExecutors.keys.map(host=>(host,0)).toMap
    logInfo(s"#####hostBlock=${hostBlock}#####")*/
  // 单位MB/s 125MB=1000Mb
  private val bandwidth = 125
  // 默认放入0
  private val hostPerformanceDiffer: mutable.HashMap[String, Double] = mutable.HashMap.empty
  private val executorScheduledTaskCount: mutable.HashMap[String, Int] = mutable.HashMap.empty
  private val executorToDurationWithoutFetch: mutable.HashMap[String, mutable.ArrayBuffer[(Long, Long)]] = mutable.HashMap.empty
  private val scheduledTaskIds: Array[Boolean] = Array.fill(tasks.length)(false)
  private val successfulTaskIds: Array[Boolean] = Array.fill(tasks.length)(false)
  private val movedTaskIds: Array[Boolean] = Array.fill(tasks.length)(false)
  // 获取executor的核心数
  private val executorCores: Int = conf.get(EXECUTOR_CORES)
  private var skipResourceOffer = false
  // 从配置文件中获取参数
  // 开始的判断的任务运行轮数
  private val ROUND: Int = conf.get("spark.round", "1").toInt
  private val COMPENSATE_TIME = conf.getOption("spark.compensateTime").getOrElse("0").toInt
  // 数据都在慢节点上时 将慢节点任务传送给快节点的数目
  private val MAX_TRANS_TIMES = conf.get("spark.maxTransTimes", "1").toInt
  // logInfo(s"ROUND = ${ROUND}")
  // 当两个executor之间的最近($executorCores)个任务的平均执行时间相差fastPerformanceThreshold倍的以上时，就认为这两个executor不是同构的
  private val fastPerformanceThreshold = 1.5
  private val slowPerformanceThreshold = 0.67


  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * @param execId      the executor Id of the offered resource
   * @param host        the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
   * @return Tuple containing:
   *         (TaskDescription of launched task if any, rejected resource due to delay scheduling?)
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
                     execId: String,
                     host: String,
                     maxLocality: TaskLocality.TaskLocality,
                     taskResourceAssignments: Map[String, ResourceInformation] = Map.empty)
  : (Option[TaskDescription], Boolean) = {
    skipResourceOffer = false
    // 判断当前的executor和host是不是对当前任务集来说不可用
    val offerExcluded = taskSetExcludelistHelperOpt.exists { excludeList =>
      excludeList.isNodeExcludedForTaskSet(host) ||
        excludeList.isExecutorExcludedForTaskSet(execId)
    }
    // 如果当前executor可用
    if (!isZombie && !offerExcluded) {
      val curTime = clock.getTimeMillis()

      // allowedLocality = 最大的数据本地性等级
      var allowedLocality = maxLocality
      // 如果所有任务的副本中的最大数据本地性等级不是NO_PREF
      if (maxLocality != TaskLocality.NO_PREF) {
        // 获得任务真正的数据本地性等级，延迟调度的入口======================
        // 因为当任务的资源在一定时间内无法满足时，会触发数据本地性降级
        // 懒更新 内部将已经完成/正在运行的任务 出队列了
        logInfo(s"=====当前任务集中的最大数据本地性等级不是NO_PREF=====")
        /*
        延迟调度入口=============================================================================
        从数据本地性等级高的 开始遍历 如果对应等级的队列中有任务
        那么就进入延迟调度 判断有没有超时 如果没有,就直接返回当前等级 因为当前等级有任务可以调度
        如果超时了(什么情况下会超时？？只要延迟调度被拒绝过 它就一直为false 就一直不会重置本地性的时间) 就进入下一个等级
        也就是说 无论现在用什么等级调度任务 延迟调度都会从高往低遍历每一个等级 如果在高等级上能够调度任务 就会用高等级
        e.g. 如果现在以ANY调度 但是NODE等级中有任务 并且没有超时 就用NODE等级
        */
        allowedLocality = getAllowedLocalityLevel(curTime)

        logInfo(s"#####各个队列中的未调度的任务数#####")
        logInfo(s"#####unscheduledTaskCount=\n${unscheduledTaskCount.mkString("\n")}#####")

        for (host <- sched.hostToExecutors.keys) {
          // 初始化一下主机阻塞的时间
          if (!hostBlockTime.contains(host)) hostBlockTime.put(host, 0)
        }
        // 过滤taskSet中已经完成或者正在运行的任务，如果有，对应host的value++
        for ((tid, taskInfo) <- taskInfos) {
          val host = taskInfo.host
          val executorId = taskInfo.executorId
          // 每个任务都会进入正在运行的状态，所以会遍历到所有任务的信息
          if (runningTasksSet.contains(tid)) {
            // logInfo(s"#####running tid=${tid},partitionId=${tasks(taskInfo.index).partitionId},host=${host},executorId=${executorId}#####")
            // 如果host/executor没有出现过
            if (!hostScheduledTaskCount.keys.exists(_ == host)) {
              hostScheduledTaskCount.put(host, 0)
            }
            if (!executorScheduledTaskCount.keys.exists(_ == executorId)) executorScheduledTaskCount.put(executorId, 0)
            // 避免重新计算
            if (!scheduledTaskIds(taskInfo.index)) {
              hostScheduledTaskCount(host) += 1
              executorScheduledTaskCount(executorId) += 1
              scheduledTaskIds(taskInfo.index) = true
            }
          }
          // 如果当前任务已经完成了
          if (taskInfo.durationWithoutFetch != 0) {
            // 当前executor第一次出现，将其加入到map中
            if (!executorToDurationWithoutFetch.keys.exists(_ == executorId)) executorToDurationWithoutFetch.put(executorId, mutable.ArrayBuffer.empty)
            if (!successfulTaskIds(taskInfo.index)) {
              executorToDurationWithoutFetch(executorId) += ((taskInfo.durationWithoutFetch, tasks(taskInfo.index).taskSize))
              successfulTaskIds(taskInfo.index) = true
            }
          }
        }
        logInfo(s"#####各个主机上调度的任务数#####")
        logInfo(s"#####hostScheduledTaskCount=\n${hostScheduledTaskCount.mkString("\n")}#####")
        logInfo(s"#####各个executor上调度的任务数#####")
        logInfo(s"#####executorScheduledTaskCount=\n${executorScheduledTaskCount.mkString("\n")}#####")
        logInfo(s"#####各个executor已经运行完毕的任务执行时间#####")
        logInfo(s"#####executorToDurationWithoutFetch=\n${executorToDurationWithoutFetch.mkString("\n")}#####")

        // 如果当前数据本地性等级不是 PROCESS_LOCAL/NODE_LOCAL 可能没有数据偏好 比如ANY/NO_PREFER(不会进入当前位置),就会出现任务偏好位置数为0的情况
        // val copyCount = Math.max(tasks(0).preferredLocations.length,1)
        val hostEstimateUnscheduledTaskCount: HashMap[String, Double] = mutable.HashMap.empty
        val realHostScheduledTaskCount = hostScheduledTaskCount.values.sum
        val realUnscheduledTaskCount = tasks.length - realHostScheduledTaskCount
        logInfo(s"真实未被调度的任务总数realUnscheduledTaskCount=${realUnscheduledTaskCount}," +
          s"真实主机上被调度的任务总数realHostScheduledTaskCount=${realHostScheduledTaskCount}#####")
        // 这里可能出现bug，unscheduledTaskCount中存放的是队列中的key，而hostScheduledTaskCount存放的只是host
        // 当队列中计算数据本地性的时候 只有某一部分节点在NODE——LOCAL中时 就会出现key找不到的情况
        for ((host, count) <- hostScheduledTaskCount if unscheduledTaskCount.contains(host)) {
          // unscheduledTaskCount存放的是所有队列中的key对应的未被调度的任务数量 目前只考虑host层 那就只用找到host就行
          // 计算的公式 主机上预估的未被调度的数目 = min(未被调度的任务数×(主机上被调度的任务数/总的被调度的任务数),host队列中的任务数)
          val estimateCount = Math.min(realUnscheduledTaskCount * count / realHostScheduledTaskCount, unscheduledTaskCount(host))
          hostEstimateUnscheduledTaskCount.put(host, estimateCount)
        }
        logInfo(s"#####hostEstimateUnscheduledTaskCount=${hostEstimateUnscheduledTaskCount}#####")
        // 初始化标识快节点的map
        hostPerformanceDiffer.clear()
        for (hostId <- hostScheduledTaskCount.keys) {
          hostPerformanceDiffer.put(hostId, 0)
        }
        // 当前主机的性能看作1
        hostPerformanceDiffer(host) = 1
        val taskCountDiff = 1.5
        // 表示executor至少跑round轮任务 因为第一轮任务都偏长 需要涉及到反序列化等操作
        // val round = 1
        // 如果想只判断每一轮的第一个任务 executorScheduledTaskCount(execId) % executorCores == 0
        // 每个executor至少要跑executorCores个任务 并且至少有一个任务跑成功了
        if (executorScheduledTaskCount.contains(execId) && executorScheduledTaskCount(execId) >= executorCores * ROUND
          // executorToDurationWithoutFetch只要存在execId 就说明execId已经至少有一个任务完成了
          && executorToDurationWithoutFetch.contains(execId)) {
          // 以当前执行任务的executor作为baseline,取最近的 $executorCores 个完成的任务作为计算目标，求 数据量/时间
          // TODO：有一种情况需要考虑 就是从慢节点迁移过来的任务 可能受数据本地行的影响 它计算的时间偏长 应该将这类任务排除在外
          val baseTimesAndSizes = executorToDurationWithoutFetch(execId).takeRight(executorCores)
          // val baseLength = baseTimesAndSizes.length
          // 总数据量大小 / 总时间
          val baseTimeSum = baseTimesAndSizes.map(_._1).sum
          val baseSizeSum = baseTimesAndSizes.map(_._2).sum
          val baseAvgCompleteRate = (baseSizeSum * 1.0 / baseTimeSum).formatted("%.2f").toDouble

          // 主机上调度的任务数>当前主机的每个executor至少运行一轮任务
          // if (hostScheduledTaskCount.contains(host) && hostScheduledTaskCount(host) > executorCores * sched.hostToExecutors(host).size && executorScheduledTaskCount.contains(execId)) {
          // val baseCount = hostScheduledTaskCount(host)
          // 平均任务完成时间=${baseAvgCompleteTime}ms
          logInfo(s"#####进入条件判断,当前executor=${execId},host=${host},数据处理速率baseAvgCompleteRate=${baseAvgCompleteRate}#####")
          // TODO: 大小核设计 会造成同一主机上的executor有性能差异
          // 遍历每一个executor(该executor至少完成了一个任务)
          for ((exec, _) <- executorToDurationWithoutFetch) {
            val comparedHost = sched.executorIdToHost(exec)
            // 如果遍历的executor和base executor处在同一个host上面 并且当前比较的executor所在的节点之前已经比较过 就没有必要比较
            if (comparedHost != host && hostPerformanceDiffer(comparedHost) == 0) {
              // for ((comparedHost, count) <- hostScheduledTaskCount) {
              val curTimesAndSizes = executorToDurationWithoutFetch(exec).takeRight(executorCores)
              val curTimeSum = curTimesAndSizes.map(_._1).sum
              val curSizeSum = curTimesAndSizes.map(_._2).sum
              // 当前比较的executor的数据处理速率
              val curAvgCompleteRate = (curSizeSum * 1.0 / curTimeSum).formatted("%.2f").toDouble
              // 两个executor之间的性能差异
              val performanceDiff = (curAvgCompleteRate / baseAvgCompleteRate).formatted("%.2f").toDouble
              // 看快的executor
              if (performanceDiff > fastPerformanceThreshold) {
                logInfo(s"=====当前节点[${host}]与快节点[${comparedHost}]性能差异($performanceDiff),计快节点=====")
                // 性能差异较大
                // hostPerformanceDiffer(comparedHost) = performanceDiff
                /*                // 获取当前比较的executor 所在的节点的executor数目
                                val execCount = sched.hostToExecutors(comparedHost).size
                                // 慢节点执行一个任务,在那一段时间内 快节点能执行的任务数 = executor的性能差异 * executor数目 * 核心数
                                val predictedHostTaskCount = (performanceDiff * execCount * executorCores).formatted("%.2f").toDouble
                                if (hostEstimateUnscheduledTaskCount.contains(comparedHost) && performanceDiff > fastPerformanceThreshold &&
                                  // 性能差异 * executor的核心数 - 快节点的任务数 >= 阈值
                                  predictedHostTaskCount - hostEstimateUnscheduledTaskCount(comparedHost) >= taskCountDiff * execCount * executorCores) {
                                  logInfo(s"#####当前executor:${execId},host=${host},比较的快executor=${exec},host=${comparedHost}将会等待,performanceDiff=${performanceDiff}, " +
                                    s"预计快节点在慢节点时间段内执行的任务数${predictedHostTaskCount} - 快节点队列中估计的任务数:${hostEstimateUnscheduledTaskCount(comparedHost)}" +
                                    s"  >= 任务数阈值${taskCountDiff}*execCount(${execCount})*executorCores(${executorCores})#####")
                                  // 表示不在当前的executor上调度任务
                                  skipResourceOffer = true
                                }*/
              } else if (performanceDiff < slowPerformanceThreshold) {
                logInfo(s"=====当前节点[${host}]与慢节点[${comparedHost}]性能差异($performanceDiff),计慢节点=====")
                // 看慢的节点
                // hostPerformanceDiffer(comparedHost) = performanceDiff
              }
              // 不论是快还是慢 都将其计入到性能差异中
              hostPerformanceDiffer(comparedHost) = performanceDiff
            }
            /*            // 节点的性能差异
                        // val comparedExecCount = sched.hostToExecutors(comparedHost).size
                        // val baseExecCount = sched.hostToExecutors(host).size
                        // 主机的任务数量差异并不能直接作为节点的性能差异 我们最终的目的是为了将慢节点的任务放到快节点上去
                        // 这里的慢 指的是<executor>慢，如果有的节点cpu主频高但是线程数少 执行的任务数相对于 主频地线程数多的节点 更少 那应该被定义为快节点 而不是慢节点
                        val hostTaskDiff = (count.toDouble / baseCount).formatted("%.2f").toDouble
                        // 避免重复计算 实际上性能差异就是 (count/comparedExecCount / basecount/baseExecCount)
                        val performanceDiff = (baseExecCount * hostTaskDiff / comparedExecCount).formatted("%.2f").toDouble
                        // 当前比较的节点对应的executor数目
                        // val execCount = sched.hostToExecutors(comparedHost).size
                        // 预计的节点执行任务数
                        val predictatedHostTaskCount = (hostTaskDiff * executorCores).formatted("%.2f").toDouble
                        logInfo(s"#####predictatedHostTaskCount=${predictatedHostTaskCount},hostTaskDiff=${hostTaskDiff},performanceDiff=${performanceDiff}," +
                          s"executorCores=${executorCores}#####")
                        // 只寻找比当前host快的
                        if (hostEstimateUnscheduledTaskCount.contains(comparedHost) && performanceDiff > fastPerformanceThreshold &&
                          // 性能差异 * executor的核心数 - 快节点的任务数 >= 阈值
                          predictatedHostTaskCount - hostEstimateUnscheduledTaskCount(comparedHost) >= taskCountDiff) {
                          hostPerformanceDiffer(comparedHost) = true
                          logInfo(s"#####当前executor:${execId},host=${host},比较的快host=${comparedHost}将会等待,performanceDiff=${performanceDiff}, " +
                            s"主机调度任务数差异(${hostTaskDiff})*executorCores(${executorCores}) - 快节点中估计的任务数:${hostEstimateUnscheduledTaskCount(comparedHost)}  >= 任务数阈值${taskCountDiff}#####")
                          // 表示不在当前的executor上调度任务
                          skipResourceOffer = true
                        }*/
          }
        }
        /*logInfo(s"#####下面统计各队列中的任务数#####")
        maxLocality match {
          case TaskLocality.PROCESS_LOCAL =>
            for ((executorName, taskPartitions) <- pendingTasks.forExecutor) {
              logInfo(s"executorName=${executorName},count=${taskPartitions.length}")
            }
          case TaskLocality.NODE_LOCAL =>
            for ((hostName, taskPartitions) <- pendingTasks.forHost) {
              logInfo(s"hostName=${hostName},count=${taskPartitions.length}")
            }
          case TaskLocality.NO_PREF =>
            logInfo(s"noPrefs,count=${pendingTasks.noPrefs.length}")
          case TaskLocality.RACK_LOCAL =>
            for ((rackName, taskPartitions) <- pendingTasks.forRack) {
              logInfo(s"rackName=${rackName},count=${taskPartitions.length}")
            }
          case TaskLocality.ANY =>
            logInfo(s"ANY,count=${pendingTasks.all.length}")
        }
        logInfo(s"#####运行成功的taskInfo#####")
        for ((tid, successfulTaskInfo) <- taskInfos if successful(successfulTaskInfo.index)) {
          logInfo(s"#####successful tid=${tid},partitionId=${tasks(successfulTaskInfo.index).partitionId}#####")
        }
        logInfo(s"#####正在运行的taskInfo#####")
        for ((tid, runningTasInfo) <- taskInfos if runningTasksSet.contains(tid)) {
          logInfo(s"#####running tid=${tid},partitionId=${tasks(runningTasInfo.index).partitionId}#####")
        }*/

        /* 如果[延迟调度]计算的数据本地性等级比之前记录的等级还要低(现在以NODE等级进行resourceOffer 但是由于在NODE等待的时间太长 已经超时了 所以返回的ANY)
       不会使用ANY 依然使用NODE*/
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          logInfo(s"#####延迟调度更新的数据本地性等级${allowedLocality} *低于* 本轮循环的数据本地性等级${maxLocality},依旧使用本轮循环等级${maxLocality}#####")
          allowedLocality = maxLocality
        }
      }
      /*
      * taskDescription包括了以下信息：
      *taskId,attemptNum,execId,tName,index,task.partitionId,
      *addedFiles,addedJars,addedArchives,task.localProperties,
      *taskResourceAssignments,serializedTask
      * */
      val taskDescription = {
        // 任务出队列，真正地把任务取出来
        // 返回的是(任务的索引,本地性等级,是否特定执行)每一次取出一个任务
        logInfo(s"==============<取出任务前>taskSet=${this}=================")
        logInfo(s"#####[延迟调度出口]准备从任务本地性等级为${allowedLocality}的队列中取1个任务#####")

        logInfo(s"#####hostPerformanceDiffer=${hostPerformanceDiffer.mkString(",")} #####")
        val currTime = clock.getTimeMillis()

        var slowHostList: Seq[(String, Double)] = Seq.empty
        // 这是对于当前exec来说 如果当前host是最慢的 那么就会导致slowHostList为空 因为当前host权值设置为1 达不到条件
        // 这里只是对于当前host来说 去找相对于当前host的慢节点
        // slowHostList = hostPerformanceDiffer.filter { case (_, value) => value < slowPerformanceThreshold}.toSeq
        slowHostList = hostPerformanceDiffer.filter { case (_, value) => value < slowPerformanceThreshold }.toSeq
        // 如果当前host对应的慢节点为空 并且 当前节点有快节点 那就将当前节点放入慢节点集合中
        if (slowHostList.isEmpty && hostPerformanceDiffer.exists { case (_, value) => value > fastPerformanceThreshold }) {
          slowHostList = Seq((host, 1.0))
        }
        logInfo(s"#####慢节点map：${slowHostList.mkString(",")}#####")
        val freeSlowHostList = slowHostList.filter {
          case (slowHost, _) => {
            // 判断慢节点是否空闲
            if (hostBlockTime.contains(slowHost)) (currTime > hostBlockTime(slowHost))
            // 如果hostBlockTime中slowHost没出现过 说明是最初的情况
            else true
          }
        }
        logInfo(s"#####空闲的慢节点map：${freeSlowHostList.mkString(",")}#####")
        var chosenSlowHost = ""
        // executorToDurationWithoutFetch.nonEmpty表示只要跑完了一个任务
        if (freeSlowHostList.nonEmpty && executorToDurationWithoutFetch.nonEmpty) {
          // 在空闲的慢节点中 随机选一个慢节点
          val randomIndex = Random.nextInt(freeSlowHostList.size)
          // chosenSlowHost = freeSlowHostList(randomIndex)._1
          chosenSlowHost = Random.shuffle(freeSlowHostList).apply(randomIndex)._1
        }
        logInfo(s"#####选中的慢节点是${chosenSlowHost} #####")

        /*var shuffledSlowHostList: Seq[(String, Double)] = mutable.Seq.empty
        shuffledSlowHostList = Random.shuffle(hostPerformanceDiffer.filter { case (_, value) => value < slowPerformanceThreshold && value != 0 }.toSeq)

        logInfo(s"#####打乱的慢节点map：${shuffledSlowHostList.mkString(",")}#####")
        // 性能平方 的倒数作为节点的权重
        val weightShuffledSlowHostList = shuffledSlowHostList.map {
          case (host, performance) => (host, (1.0 / performance).formatted("%.2f").toDouble)
        }
        val freeWeightShuffledSlowHostList = weightShuffledSlowHostList.filter {
          case (slowHost, _) => {
            // 判断慢节点是否空闲
            if (hostBlockTime.contains(slowHost)) (currTime > hostBlockTime(slowHost))
            // 如果hostBlockTime中slowHost没出现过 说明是最初的情况
            else true
          }
        }
        if (weightShuffledSlowHostList.nonEmpty) logInfo(s"#####已排序的慢节点map权重为：${weightShuffledSlowHostList.mkString(",")}#####")
        if (freeWeightShuffledSlowHostList.nonEmpty) logInfo(s"#####已排序的空闲慢节点map权重为：${freeWeightShuffledSlowHostList.mkString(",")}#####")
        // 借助轮盘赌的思想 随机选择一个慢节点 慢节点的权重越大 被选中的几率越高
        val weightSum = freeWeightShuffledSlowHostList.map(_._2).sum
        // Math.random().formatted("%.2f").toDouble
        val random = Math.random().formatted("%.2f").toDouble
        val probability = random * weightSum
        var preSum = 0.0
        var chosenSlowHost = ""
        var endFor = false
        // 开始轮盘赌
        for (weightSortedSlowHost <- freeWeightShuffledSlowHostList if (!endFor)) {
          preSum += weightSortedSlowHost._2
          if (probability <= preSum) {
            chosenSlowHost = weightSortedSlowHost._1
            endFor = true
          }
        }
        // if(freeWeightShuffledSlowHostList.nonEmpty) chosenSlowHost = freeWeightShuffledSlowHostList.head._1
        logInfo(s"#####random=${random},probability=${probability},选中的节点是${chosenSlowHost} #####")*/

        // TODO 获得选择的慢节点的快节点 目前hostPerformanceDiffer是以当前节点为基础的 将其改成以选择的host为基础的
        /*        val fastHostList = hostPerformanceDiffer.filter(_._2 > fastPerformanceThreshold).keys.toSeq
                val shuffledFastHostList = Random.shuffle(fastHostList)*/
        var shuffledFastHostListToSlow: Seq[String] = mutable.Seq.empty
        if (chosenSlowHost.nonEmpty) {
          var performanceToSlow = hostPerformanceDiffer(chosenSlowHost)
          // 先给定一个固定小的值0.1吧
          if (performanceToSlow == 0) performanceToSlow = 0.1
          val hostPerformanceDifferToSlow = hostPerformanceDiffer.map {
            case (host, performance) => (host, (performance / performanceToSlow).formatted("%.2f").toDouble)
          }
          // 这里并不能像选择慢节点一样 先筛选空闲的快节点 因为后面的数据本地性要考查所有的快节点 只要数据在快节点上 无论其是否空闲 都不会移动该任务
          val fastHostListToSlow = hostPerformanceDifferToSlow.filter(_._2 > fastPerformanceThreshold).keys.toSeq
          logInfo(s"#####chosenSlowHost=${chosenSlowHost},hostPerformanceDifferToSlow=${hostPerformanceDifferToSlow}#####")
          logInfo(s"#####fastHostListToSlow=${fastHostListToSlow}#####")
          shuffledFastHostListToSlow = Random.shuffle(fastHostListToSlow)
        }
        /**
         * Custom modifications by jaken
         * 这个时候已经统计出 相对于当前节点的快节点了
         * 可以从慢节点中找一找 在快节点没有数据本地性的任务 将其转移到快节点去 好好利用前面这一段时间的带宽 用带宽来换时间
         */

        // 找到当前等级对应的任务队列
        var queueMap: HashMap[String, ArrayBuffer[Int]] = HashMap.empty
        allowedLocality match {
          // TODO: PROCESS_LOCAL等级先不作考虑
          //                  case PROCESS_LOCAL =>
          //                    queueMap = pendingTasks.forExecutor
          case NODE_LOCAL =>
            queueMap = pendingTasks.forHost
            logInfo(s"#####当前选择的慢节点chosenSlowHost=${chosenSlowHost},对应打乱的快节点集合[${shuffledFastHostListToSlow.mkString(",")}]")
            /*logInfo(s"#####当前节点host=${host},对应打乱的快节点集合[${shuffledFastHostList.mkString(",")}]" +
              s"运行任务Pid=${index},偏好位置=${task.preferredLocations.map(_.host)}#####")*/
            // 任务的副本数 大于 慢节点的个数+它本身(1) 表明总会有任务在快节点上 用于转移任务
            val alwaysInFast = (tasks.head.preferredLocations.size > (hostPerformanceDiffer.size - shuffledFastHostListToSlow.size))
            var tarnsTimes = 0
            if (queueMap.nonEmpty && queueMap.contains(chosenSlowHost)) {
              // 如果本次调度没有跳过 && 当前节点对应存在快节点 && 当前节点网络空闲baseHostFree
              if (!skipResourceOffer && shuffledFastHostListToSlow.nonEmpty) {
                logInfo(s"=====被选择的慢节点空闲,调度正常进行,快节点不为空,可以预迁移任务=====")
                // 从当前节点的队列中 从前往后找(相当于优先找最晚执行的任务) 如果任务的数据全在慢节点上 就将其迁移到快节点上 找到要迁移的任务
                val queueMapSize = queueMap(chosenSlowHost).size
                // TODO 也许不用每次都从0开始 记录一下每个节点开始转移的下标
                var indexOffset = 0
                var endWhile = false
                // 首先要找到要迁移的任务
                /*
                * 因为走到这一步的时候 已经默认没有跳过本次任务调度 && 已经将任务从末尾取出来了
                * */
                while (indexOffset < queueMapSize && !endWhile) {
                  // curIndex是任务的partitionId
                  val curIndex = queueMap(chosenSlowHost)(indexOffset)
                  logInfo(s"#####curIndex=${curIndex},movedTaskIds(curIndex)=${movedTaskIds(curIndex)}#####")
                  // 我们并没有在慢节点上删除任务 因为移动到快节点的任务会很快被执行 所以为了避免慢节点重复将该任务迁移到快节点 需要判断要迁移的任务是否已经被迁移过
                  // !scheduledTaskIds(curIndex) 已经被执行
                  if (!movedTaskIds(curIndex)) {
                    // 拿到任务的数据所在地
                    val dataLocations = tasks(curIndex).preferredLocations.map(parseLocationToString(_, allowedLocality))
                    logInfo(s"=====当前遍历任务partitionId=${curIndex},数据本地性为[${dataLocations.mkString(",")}]=====")
                    val fastHostL: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
                    var dataInFast = false
                    // 遍历每一个快节点 只要有数据在快节点上 那么就没必要继续考虑当前任务了
                    for (fastHost <- shuffledFastHostListToSlow if !dataInFast) {
                      // 如果数据没有在当前遍历的快节点上 可以将当前任务迁移到该快节点上
                      if (!dataLocations.contains(fastHost)) {
                        // 将节点加入当前能够迁移的快节点集合中
                        fastHostL += fastHost
                      } else {
                        // 否则 有数据在快节点上 表明当前任务不能迁移 因为它可以在快节点上用好的本地性等级计算
                        dataInFast = true
                        if (!queueMap.contains(fastHost)) queueMap.put(fastHost, ArrayBuffer.empty)
                        // 一次调度最多只让迁移MAX_TRANS_TIMES次
                        if (tarnsTimes < MAX_TRANS_TIMES) {
                          // 在快节点的末尾加入任务 因为总会有任务在快节点上 所以不会涉及数据传输
                          /*           // 将其从慢节点中移除 这里没有使用remove 因为其代价有点大 直接让它等于末尾的任务
                                     val replaceIndex = queueMap(chosenSlowHost)(queueMapSize-1)
                                     queueMap(chosenSlowHost)(indexOffset) = replaceIndex*/
                          queueMap(fastHost) += curIndex
                          tarnsTimes += 1
                          // 记录当前任务被移动了 防止重复考虑
                          movedTaskIds(curIndex) = true
                          // movedTaskIds(replaceIndex) = true
                          logInfo(s"=====数据在快节点上,直接将任务${curIndex}分给快节点${fastHost}=====")
                        }
                        // 如果总是会有任务在快节点上
                        if (alwaysInFast) {
                          /*// 在慢节点中将任务移除
                          queueMap(chosenSlowHost).remove(indexOffset)*/
                          tarnsTimes += 1
                          logInfo(s"#####总会有任务在快节点上 迁移${MAX_TRANS_TIMES}次任务 #####")
                          // 当转移了MAX_TRANS_TIMES次之后 退出循环
                          if (tarnsTimes >= MAX_TRANS_TIMES) endWhile = true
                        } else logInfo(s"=====有数据在快节点上,考虑下一任务=====")
                      }
                    }
                    if (!dataInFast) {
                      if (fastHostL.nonEmpty) logInfo(s"=====数据位置满足迁移的快节点有[${fastHostL.mkString(",")}]=====")
                      var allFastHostBusy = true
                      for (fh <- fastHostL) {
                        // 只要有一个节点空闲 就可以尝试将任务迁移过去
                        if (!hostBlockTime.contains(fh) || currTime > hostBlockTime(fh)) {
                          allFastHostBusy = false
                        }
                      }
                      // 快节点都忙的话 要提前退出 不能迁移 (当满足数据位置的节点为空时 不应当提前结束 而应当考虑下一个任务)
                      if (fastHostL.nonEmpty && allFastHostBusy) {
                        logInfo(s"=====快节点都忙,跳过本轮任务转移=====")
                        endWhile = true
                      }
                      else {
                        // logInfo(s"#####当前节点对应的快节点fastHostL:${fastHostL.mkString(",")}#####")
                        // 如果migrateTaskToFastNode返回true 就表明任务成功迁移到了快节点
                        endWhile = migrateTaskToFastNode(fastHostL, currTime, queueMap, curIndex, tasks(curIndex), chosenSlowHost)
                        // 这里也许并不需要将任务从数据所在地的节点移除 因为既然能够迁移成功 我们是把任务放在快节点末尾 快节点大概率可以比慢节点优先执行
                      }
                    }
                  }
                  indexOffset += 1
                }
              }
            }
          case PROCESS_LOCAL =>
            queueMap = pendingTasks.forExecutor
            var chosenSlowExecutor = ""
            var freeShuffledFastHostListToSlow: Seq[String] = Seq.empty
            var shuffledFastExecutorListToSlow: Seq[String] = Seq.empty
            // 选择慢节点打乱的第一个executor 找到快的executor
            if (chosenSlowHost.nonEmpty) {
              chosenSlowExecutor = Random.shuffle(sched.hostToExecutors(chosenSlowHost)).head
              // 筛选出空闲的快节点 因为PROCESS_LOCAL并不需要判断数据本地性
              freeShuffledFastHostListToSlow = shuffledFastHostListToSlow.filter {
                case (fastHost) => {
                  // 判断慢节点是否空闲
                  if (hostBlockTime.contains(fastHost)) (currTime > hostBlockTime(fastHost))
                  // 如果hostBlockTime中slowHost没出现过 说明是最初的情况
                  else true
                }
              }
              shuffledFastExecutorListToSlow = Random.shuffle(freeShuffledFastHostListToSlow.flatMap(sched.hostToExecutors(_)))
              logInfo(s"#####chosenSlowExecutor=${chosenSlowExecutor}," +
                s"freeShuffledFastHostListToSlow=${freeShuffledFastHostListToSlow.mkString(",")}," +
                s"shuffledFastExecutorListToSlow=${shuffledFastExecutorListToSlow.mkString(",")}#####")
            }

            if (queueMap.nonEmpty && queueMap.contains(chosenSlowExecutor)) {
              // 如果本次调度没有跳过 && 当前节点对应存在快节点 && 当前节点网络空闲baseHostFree
              if (!skipResourceOffer && shuffledFastExecutorListToSlow.nonEmpty) {
                // logInfo(s"=====被选择的慢节点空闲,调度正常进行,快节点不为空,可以预迁移任务=====")
                // 从当前节点的队列中 从前往后找(相当于优先找最晚执行的任务) 如果任务的数据全在慢节点上 就将其迁移到快节点上 找到要迁移的任务
                val queueMapSize = queueMap(chosenSlowExecutor).size
                // TODO 也许不用每次都从0开始 记录一下每个节点开始转移的下标
                var indexOffset = 0
                var endWhile = false
                // 首先要找到要迁移的任务 然后将任务迁移过去
                while (indexOffset < queueMapSize && !endWhile) {
                  // curIndex是任务的partitionId
                  val curIndex = queueMap(chosenSlowExecutor)(indexOffset)
                  // 我们并没有在慢节点上删除任务 因为移动到快节点的任务会很快被执行 所以为了避免慢节点重复将该任务迁移到快节点 需要判断要迁移的任务是否已经被迁移过
                  // !scheduledTaskIds(curIndex) 已经被执行
                  if (!movedTaskIds(curIndex)) {
                    // 拿到任务的数据所在地 这里是executor本地性 就返回的是executorId
                    val dataLocations = tasks(curIndex).preferredLocations.map(parseLocationToString(_, allowedLocality))
                    logInfo(s"=====当前遍历任务partitionId=${curIndex},数据本地性为[${dataLocations.mkString(",")}]=====")
                    // 选快executor的第一个 把任务迁移过去
                    val chosenFastExecutor = shuffledFastExecutorListToSlow.head
                    if (!queueMap.contains(chosenFastExecutor)) queueMap.put(chosenFastExecutor, ArrayBuffer.empty)
                    queueMap(chosenFastExecutor) += curIndex
                    logInfo(s"#####将任务partitionId=${curIndex}从慢executor${chosenSlowExecutor}迁移到快executor${chosenFastExecutor}#####")
                    endWhile = true
                    movedTaskIds(curIndex) = true
                    val blockTime = ((tasks(curIndex).taskSize >> 20) * 1000 / bandwidth) + currTime + COMPENSATE_TIME
                    logInfo(s"#####当前系统时间${currTime}ms,任务迁移之后的阻塞截止时间: blockTime = ${blockTime}ms#####")
                    hostBlockTime(chosenSlowHost) = blockTime
                    hostBlockTime(sched.executorIdToHost(chosenFastExecutor)) = blockTime
                  }
                  indexOffset += 1
                }
              }
            }
          // 如果是其他情况 先不管 直接返回
          case _ => queueMap = HashMap.empty
        }
        // 做一个注释

        dequeueTask(execId, host, allowedLocality)
          .map {
            // (原生Spark)这个时候 任务已经从队列中取出来了 这里如果本次调度需要跳过,我们还是把它加回去了 需要针对任务数据所在地的性能去考虑是否删除当前任务
            case (index, taskLocality, speculative) =>
              // Found a task; do some bookkeeping and return a task description
              // 拿到真正的任务
              // 这里拿到的index 其实是任务队列里对应的partitionId
              val task = tasks(index)

              val taskId = sched.newTaskId()
              logInfo(s"#####真正取出任务本地性等级为${taskLocality},taskId=${taskId},partitionId=${index},execId=${execId},host=${host},task=${tasks(index)}#####")
              logInfo(s"==============<取出任务后>taskSet=${this}=================")
              // Do various bookkeeping
              copiesRunning(index) += 1
              val attemptNum = taskAttempts(index).size
              // 封装任务的信息
              val info = new TaskInfo(taskId, index, attemptNum, curTime,
                execId, host, taskLocality, speculative)
              taskInfos(taskId) = info
              taskAttempts(index) = info :: taskAttempts(index)
              // 从3.1版本开始 如果配置了legacyLocalityWaitReset=true 表示使用旧版的调度策略(默认为false)
              // 旧版本延迟调度 也就是只要有任务从队列中取出 就重置数据本地性等待的时间
              if (legacyLocalityWaitReset && maxLocality != TaskLocality.NO_PREF) {
                // 重新设置延迟调度的时间，这里更新了调度 当前数据本地性等级的时间;
                // 与数据本地性降级相关
                resetDelayScheduleTimer(Some(taskLocality))
              }
              // Serialize and return the task
              val serializedTask: ByteBuffer = try {
                ser.serialize(task)
              } catch {
                // If the task cannot be serialized, then there's no point to re-attempt the task,
                // as it will always fail. So just abort the whole task-set.
                // 如果任务不能被序列化，就没有必要继续安排该任务进行调度了，因为它永远会失败，所以放弃整个任务集
                case NonFatal(e) =>
                  val msg = s"Failed to serialize task $taskId, not attempting to retry it."
                  logError(msg, e)
                  abort(s"$msg Exception during serialization: $e")
                  throw new TaskNotSerializableException(e)
              }
              // 如果任务序列化后的大小超过了警报阈值，则会警报
              if (serializedTask.limit() > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024 &&
                !emittedTaskSizeWarning) {
                emittedTaskSizeWarning = true
                logWarning(s"Stage ${task.stageId} contains a task of very large size " +
                  s"(${serializedTask.limit() / 1024} KiB). The maximum recommended task size is " +
                  s"${TaskSetManager.TASK_SIZE_TO_WARN_KIB} KiB.")
              }
              // 将当前任务加入到待运行的集合中
              addRunningTask(taskId)

              // We used to log the time it takes to serialize the task, but task size is already
              // a good proxy to task serialization time.
              // val timeTaken = clock.getTime() - startTime
              val tName = taskName(taskId)
              logInfo(s"Starting $tName ($host, executor ${info.executorId}, " +
                s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit()} bytes) " +
                s"taskResourceAssignments ${taskResourceAssignments}")

              // 告诉DAGScheduler任务开始执行了，DAGScheduler会监视任务的运行情况==================
              sched.dagScheduler.taskStarted(task, info)
              // 返回封装的TaskDescription对象
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
                taskResourceAssignments,
                serializedTask)
          }
      }
      val hasPendingTasks = pendingTasks.all.nonEmpty || pendingSpeculatableTasks.all.nonEmpty
      // 如果走遍了所有的数据本地性等级 任务都没有被执行 但是 all任务队列中有任务(存在没有被调度的任务),表示任务调度请求被拒绝了
      val hasScheduleDelayReject =
        taskDescription.isEmpty &&
          maxLocality == TaskLocality.ANY &&
          hasPendingTasks
      if (hasScheduleDelayReject) {
        logInfo(s"=====没有运行新任务 & 数据本地性等级=ANY & 队列中存在任务,说明此次任务调度被拒了,没有使用任何资源=====")
      }
      (taskDescription, hasScheduleDelayReject)
    }
    else {
      (None, false)
    }
  }

  def taskName(tid: Long): String = {
    val info = taskInfos.get(tid)
    assert(info.isDefined, s"Can not find TaskInfo for task (TID $tid)")
    s"task ${info.get.id} in stage ${taskSet.id} (TID $tid)"
  }

  private def maybeFinishTaskSet(): Unit = {
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
   */
  // 获得任务临近计算时，真正的数据本地性等级
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    // 会删除forXXX map中已经被调度的任务 只要有没有完成的任务就会返回true
    // forXXX map中的任务都运行完毕了就返回false，懒删除已经调度的 或已经完成的任务
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      var indexOffset = pendingTaskIds.size
      // 从后往前判断任务是否执行完毕
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        // copiesRunning(index) == 0表示任务没有被执行 也 没有运行成功就返回ture(也就是任务没跑过)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          // 否则就表示运行完了，将其从队列中删除
          logInfo(s"=====tasksNeedToBeScheduledFrom 任务(partitionId=${index})跑过了,将其从任务队列中移除=====")
          pendingTaskIds.remove(indexOffset)
        }
      }
      // forXXX map中的任务都运行完毕了就返回false
      false
    }

    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    // pendingTasks中有任务可以被调度,就返回true,删除队列中值为空的 key
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      // 在这里 遍历每个等级对应的map<string,arraybuffer>
      val hasTasks = pendingTasks.exists {
        // id是executor/host/rack的id tasks为可以放置的任务
        case (id: String, tasks: ArrayBuffer[Int]) =>
          // 会删除一些任务
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            logInfo(s"#####队列中的任务都已经完成了hostname/executorname=${id}加入emptyKey #####")
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
      // 删除完成的任务
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    /**
     * TaskSetManager的构造方法，首次计算数据本地性
     * 注意TaskSetManager中包含了所有的executor的信息
     * 默认currentLocalityIndex = 0,
     */
    // 遍历当前任务集中 所有任务的数据本地性等级情况，请注意!!!
    // 当任数据本地性等级还没有降到最低,就进入while循环,如果降到最低了,那就没有延迟调度的必要了,因为不能再降级了
    /*从数据本地性等级高的 开始遍历 如果对应等级的队列中有任务
    那么就进入延迟调度 判断有没有超时 如果没有,就直接返回当前等级 因为当前等级有任务可以调度
    如果超时了 就进入下一个等级
    也就是说 无论现在用什么等级调度任务 延迟调度都会从高往低遍历每一个等级 如果在高等级上能够调度任务 就会用高等级
    */
    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      // 根据最高的数据本地性等级来获取任务
      // 也就是说，优先执行数据本地性高的任务

      // moreTasks=true 表示forXXX map中还有任务需要被调度
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        // 当前数据本地性最高的等级为PROCESS_LOCAL 就去forExecutor找任务
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasks.forExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasks.forHost)
        case TaskLocality.NO_PREF => pendingTasks.noPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasks.forRack)
      }
      // 如果forXXX map中没有任务能被调度了
      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        lastLocalityWaitResetTime = curTime
        logInfo(s"=====当前数据本地性等级${myLocalityLevels(currentLocalityIndex)}的任务队列中已经没有任务了,进入下一个任务本地性等级${myLocalityLevels(currentLocalityIndex + 1)}=====")
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        // 索引++，数据本地性降级
        currentLocalityIndex += 1
      }
      // 当前数据本地性等级 还有任务可以被调度
      // 但是任务等待的时间 超过了当前任务本地性对应的最大等待时间，会触发数据本地性降级==========
      else if (curTime - lastLocalityWaitResetTime >= localityWaits(currentLocalityIndex)) {
        logInfo(s"=====[延迟调度]当前数据本地性等级${myLocalityLevels(currentLocalityIndex)}的任务队列中存在任务," +
          s"但是在当前等级下等待的时间${curTime - lastLocalityWaitResetTime}ms,已经超过了${localityWaits(currentLocalityIndex)}ms," +
          s"currentLocalityIndex++ =${currentLocalityIndex + 1},进入下一个任务本地性等级${myLocalityLevels(currentLocalityIndex + 1)}=====")
        // Jump to the next locality level, and reset lastLocalityWaitResetTime so that the next
        // locality wait timer doesn't immediately expire
        lastLocalityWaitResetTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        // 索引++，数据本地性降级
        currentLocalityIndex += 1
      }
      else {
        // 当前数据本地性等级 还有任务可以被调度
        logInfo(s"=====[延迟调度]当前数据本地性等级${myLocalityLevels(currentLocalityIndex)}的任务队列中存在任务," +
          s"当前等级下等待的时间${curTime - lastLocalityWaitResetTime}ms,还未超过${localityWaits(currentLocalityIndex)}ms,数据本地性不降级,维持当前等级=====")
        return myLocalityLevels(currentLocalityIndex)
      }
    }

    logInfo(s"=====当前数据本地性等级已经为最低等级了,没有延迟调度的必要了=====")
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
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

  // 返回任务集中不能调度的一个任务的index
  // 如果有任务集中有一个任务不能被调度 就说明这个任务集不能被调度
  private[scheduler] def getCompletelyExcludedTaskIfAny(
                                                         hostToExecutors: HashMap[String, HashSet[String]]): Option[Int] = {
    taskSetExcludelistHelperOpt.flatMap {
      taskSetExcludelist =>
        val appHealthTracker = healthTracker.get
        // Only look for unschedulable tasks when at least one executor has registered. Otherwise,
        // task sets will be (unnecessarily) aborted in cases when no executors have registered yet.
        // 只有当所有的节点中存在executor的时候，才判断是不是有不能执行的任务
        if (hostToExecutors.nonEmpty) {
          // find any task that needs to be scheduled
          // 从all HashMap中从后往前找 找到一个阻塞的任务
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
          // 如果阻塞的任务不能够被调度 那么返回该任务的索引
          pendingTask.find {
            // 任务在任务集中的index
            indexInTaskSet =>
              // try to find some executor this task can run on.  Its possible that some *other*
              // task isn't schedulable anywhere, but we will discover that in some later call,
              // when that unschedulable task is the last task remaining.
              // 尝试找到这个task能够运行的executor
              hostToExecutors.forall {
                case (host, execsOnHost) =>
                  // Check if the task can run on the node
                  val nodeExcluded =
                    appHealthTracker.isNodeExcluded(host) ||
                      taskSetExcludelist.isNodeExcludedForTaskSet(host) ||
                      taskSetExcludelist.isNodeExcludedForTask(host, indexInTaskSet)
                  if (nodeExcluded) {
                    true
                  }
                  else {
                    // Check if the task can run on any of the executors
                    execsOnHost.forall {
                      exec =>
                        appHealthTracker.isExecutorExcluded(exec) ||
                          taskSetExcludelist.isExecutorExcludedForTaskSet(exec) ||
                          taskSetExcludelist.isExecutorExcludedForTask(exec, indexInTaskSet)
                    }
                  }
              }
          }
        }
        else {
          None
        }
    }
  }

  // 放弃该taskSet 因为其中的一个task不能被运行
  private[scheduler] def abortSinceCompletelyExcludedOnFailure(indexInTaskSet: Int): Unit = {
    taskSetExcludelistHelperOpt.foreach { taskSetExcludelist =>
      val partition = tasks(indexInTaskSet).partitionId
      abort(
        s"""
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
    val index = info.index
    // 如果任务已经成功执行了并且被其他用户终止了
    // Check if any other attempt succeeded before this and this attempt has not been handled
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

    // 将任务设置为完成状态,内部会获取当前时间作为任务的执行完成时间
    info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    // 如果开启了推测执行的选项 将当前任务的执行时间记录到中位数堆中
    if (speculationEnabled) {
      successfulTaskDurations.insert(info.duration)
    }
    removeRunningTask(tid)

    // Kill any other attempts for the same task (since those are unnecessary now that one
    // attempt completed successfully).
    // 如果某任务完成了 那么它的所有尝试(attempt)都应当停止
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for ${taskName(attemptInfo.taskId)}" +
        s" on ${attemptInfo.host} as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      killedByOtherAttempt += attemptInfo.taskId
      // 调用杀死任务的方法
      sched.backend.killTask(
        attemptInfo.taskId,
        attemptInfo.executorId,
        interruptThread = true,
        reason = "another attempt succeeded")
    }
    // 将任务加入到successful数组中
    if (!successful(index)) {
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      tasksSuccessful += 1
      // 这里已经有任务执行完毕的时间了，info.duration会计算任务开始运行的时间和任务的完成时间 二者相减就得到了任务的执行时间
      logInfo(s"#####任务的完成时间: Finished ${taskName(info.taskId)} in ${info.duration} ms " +
        s"on ${info.host} (executor ${info.executorId}) ($tasksSuccessful/$numTasks)#####")

      /**
       * Custom modifications by jaken
       * 打印运行完成的任务信息 和 正在运行的任务信息 用来计算异构节点之间的性能差异
       */
      /*      logInfo(s"#####运行成功的taskInfo#####")
            for ((tid, info) <- taskInfos if successful(info.index)) {
              logInfo(s"#####tid=${tid},partitionId=${tasks(info.index).partitionId},taskInfo=${info}#####")
            }
            logInfo(s"#####正在运行的tasksSet,count=${runningTasks}#####")
            logInfo(s"#####runningTasksSet=${runningTasksSet}#####")
            logInfo(s"#####<单个任务运行完毕>taskSet=${this}#####")*/

      // 当所有任务都完成了 就设置为僵尸模式
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
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates,
      result.metricPeaks, info)
    // 每次任务完成之后 都判断当前任务集是不是已经完成了
    maybeFinishTaskSet()
  }

  private[scheduler] def markPartitionCompleted(partitionId: Int): Unit = {
    partitionToIndex.get(partitionId).foreach { index =>
      if (!successful(index)) {
        tasksSuccessful += 1
        successful(index) = true
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
    if (info.failed || info.killed) {
      return
    }
    removeRunningTask(tid)
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

      case e: TaskFailedReason => // TaskResultLost and others
        logWarning(failureReason)
        None
    }

    if (tasks(index).isBarrier) {
      isZombie = true
    }

    sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, metricPeaks, info)

    if (!isZombie && reason.countTowardsTaskFailures) {
      assert(null != failureReason)
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
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        // We may have a running task whose partition has been marked as successful,
        // this partition has another task completed in another stage attempt.
        // We treat it as a running task and will call handleFailedTask later.
        if (successful(index) && !info.running && !killedByOtherAttempt.contains(tid)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(
            tasks(index), Resubmitted, null, Seq.empty, Array.empty, info)
        }
      }
    }
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
    logInfo(s"=====executor(Id=${execId})丢失时重新计算数据本地性=====")
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
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
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
    if (numSuccessfulTasks >= minFinishedForSpeculation) {
      val time = clock.getTimeMillis()
      // successfulTaskDurations.median方法找到已经完成的任务的执行时间的中位数
      val medianDuration = successfulTaskDurations.median
      val threshold = max(speculationMultiplier * medianDuration, minTimeToSpeculation)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for (tid <- runningTasksSet) {
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
    } else if (speculationTaskDurationThresOpt.isDefined && speculationTasksLessEqToSlots) {
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
   */
  /*
  * 数据本地性分类PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
  * 这里返回的taskSet的数据本地性等级 比如:
  *  若NO_PREF对应的队列没有任务，levels = [PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY]
  * */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    //forExecutor : [executor，[task1,task2,task3]] 表示executor中有task1 2 3 的数据
    // 如果task的数据已经在executor中，就是PROCESS_LOCAL等级
    // forExecutor中有一个活着的executor 就levels += PROCESS_LOCAL
    //    logInfo(s"=====computeValidLocalityLevels(),计算任务的数据本地性等级======")
    //    logInfo(s"=====pendingTasks.forExecutor=${pendingTasks.forExecutor}=====")
    //    logInfo(s"=====pendingTasks.forHost=${pendingTasks.forHost}=====")
    //    logInfo(s"=====pendingTasks.noPrefs=${pendingTasks.noPrefs}=====")
    //    logInfo(s"=====pendingTasks.forRack=${pendingTasks.forRack}=====")
    //    logInfo(s"=====pendingTasks.all=${pendingTasks.all}=====")
    // 改任务本地性等级计算
    if (!pendingTasks.forExecutor.isEmpty) {
      if (pendingTasks.forExecutor.keySet.exists(sched.isExecutorAlive(_))) {
        levels += PROCESS_LOCAL
      } else {
        logInfo(s"=====forExecutor不为空,但队列中记录的executor均 未注册/dead/不存在,不加入PROCESS_LOCAL等级=====\n")
      }
    }
    //    if (!pendingTasks.forExecutor.isEmpty &&
    //      pendingTasks.forExecutor.keySet.exists(sched.isExecutorAlive(_))) {
    //      levels += PROCESS_LOCAL
    //    }
    if (!pendingTasks.forHost.isEmpty) {
      logInfo(s"#####pendingTasks.forHost.keySet=${pendingTasks.forHost.keySet}#####")
      if (pendingTasks.forHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
        levels += NODE_LOCAL
      } else {
        logInfo(s"=====forHost不为空,但队列中所有host的executor均 未注册/dead/不存在,不加入NODE_LOCAL等级=====\n")
      }
    }
    // 如果task的数据在同一个worker中，就是NODE_LOCAL等级
    //    if (!pendingTasks.forHost.isEmpty &&
    //      pendingTasks.forHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
    //      levels += NODE_LOCAL
    //    }
    // 如果task放哪都一样，就是NO_PREF等级
    if (!pendingTasks.noPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasks.forRack.isEmpty) {
      if (pendingTasks.forRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
        levels += RACK_LOCAL
      } else {
        logInfo(s"=====forRack不为空,但队列中所有rack的host均 未注册/dead/不存在,不加入RACK_LOCAL等级=====\n")
      }
    }
    // 如果task的数据在同一个rack中，就是RACK_LOCAL等级
    //    if (!pendingTasks.forRack.isEmpty &&
    //      pendingTasks.forRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
    //      levels += RACK_LOCAL
    //    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  def executorDecommission(execId: String): Unit = {
    logInfo(s"=====executor(Id=${execId})退出时重新计算数据本地性=====")
    recomputeLocality()
  }

  def recomputeLocality(): Unit = {
    // A zombie TaskSetManager may reach here while executorLost happens
    if (isZombie) return
    // 获取当前数据本地性的等级(用数字表示,越小表示等级越高)
    //logInfo(s"=====当前数据本地性数组myLocalityLevels=${myLocalityLevels.mkString("Array(", ", ", ")")}=====")
    val previousLocalityIndex = currentLocalityIndex
    //logInfo(s"=====11.取出当前数据本地性索引currentLocalityIndex=${currentLocalityIndex}=====")
    // myLocalityLevels: 计算taskSet中的每一个任务的数据本地性等级，返回一个数组，数组的大小 = taskSet中任务的数目
    // 取出任务对应的数据本地性等级
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    //logInfo(s"=====11.当前数据本地性等级为previousLocalityLevel=${previousLocalityLevel}=====")
    val previousMyLocalityLevels = myLocalityLevels

    myLocalityLevels = computeValidLocalityLevels()
    //logInfo(s"=====11.调用computeValidLocalityLevels方法重新计算数据本地性数组=${myLocalityLevels.mkString("Array(", ", ", ")")}=====")

    localityWaits = myLocalityLevels.map(getLocalityWait)
    //logInfo(s"=====[延迟调度相关]数据本地性数组对应的最大等待时间=${localityWaits.mkString("Array(", ", ", ")")}=====")

    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
    //logInfo(s"=====重新计算当前的数据本地性索引currentLocalityIndex=${currentLocalityIndex}=====")

    if (currentLocalityIndex > previousLocalityIndex) {
      // SPARK-31837: If the new level is more local, shift to the new most local locality
      // level in terms of better data locality. For example, say the previous locality
      // levels are [PROCESS, NODE, ANY] and current level is ANY. After recompute, the
      // locality levels are [PROCESS, NODE, RACK, ANY]. Then, we'll shift to RACK level.
      logInfo(s"=====重新计算后的索引${currentLocalityIndex}>先前的索引${previousLocalityIndex}=====")
      currentLocalityIndex = getLocalityIndex(myLocalityLevels.diff(previousMyLocalityLevels).head)
      logInfo(s"=====重新更新当前的数据本地性索引currentLocalityIndex=${currentLocalityIndex}=====")
    }
  }

  def executorAdded(): Unit = {
    logInfo(s"=====executor添加时重新计算数据本地性=====")
    // 重新计算数据本地性
    recomputeLocality()
  }

  override def toString: String = s"TaskSetManager(stageId=$stageId,name=$name,myLocalityLevels=${myLocalityLevels.mkString("Array(", ", ", ")")}," +
    s"currentLocalityIndex=$currentLocalityIndex,numTasks=$numTasks \n" +
    //    s"前10个tasks=${tasks.take(10).mkString("Array(", ", ", ")")}" +
    s"前50个pendingTasks.forExecutor=\n${pendingTasks.forExecutor.take(50).mkString("\n")}\n" +
    s"前50个pendingTasks.forHost=\n${pendingTasks.forHost.take(50).mkString("\n")}\n" +
    s"前50个pendingTasks.noPrefs=\n${pendingTasks.noPrefs.take(50)}\n" +
    s"前50个pendingTasks.forRack=\n${pendingTasks.forRack.take(50).mkString("\n")}\n" +
    s"前50个pendingTasks.all=\n${pendingTasks.all.take(50)}\n" +
    s")"
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KIB = 1000
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
  // 真正操作起来 类似于栈，这样能够很快检测出失败的任务

  // Set of pending tasks for each executor.
  // 形式为[executor，[task1,task2,task3]]
  val forExecutor = new HashMap[String, ArrayBuffer[Int]]
  // Set of pending tasks for each host. Similar to pendingTasksForExecutor, but at host level.
  val forHost = new HashMap[String, ArrayBuffer[Int]]
  // Set containing pending tasks with no locality preferences.
  var noPrefs = new ArrayBuffer[Int]
  // Set of pending tasks for each rack -- similar to the above.
  val forRack = new HashMap[String, ArrayBuffer[Int]]
  // Set containing all pending tasks (also used as a stack, as above).
  var all = new ArrayBuffer[Int]
}
