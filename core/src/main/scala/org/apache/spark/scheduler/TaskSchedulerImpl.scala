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
import java.util.{Timer, TimerTask}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap, HashSet}
import scala.util.Random
import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.internal.config._
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.{ANY, TaskLocality}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a `LocalSchedulerBackend` and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * submitTasks method.
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.  This class is called from many threads, notably:
 * * The DAGScheduler Event Loop
 * * The RPCHandler threads, responding to status updates from Executors
 * * Periodic revival of all offers from the CoarseGrainedSchedulerBackend, to accommodate delay
 * scheduling
 * * task-result-getter threads
 *
 * CAUTION: Any non fatal exception thrown within Spark RPC framework can be swallowed.
 * Thus, throwing exception in methods like resourceOffers, statusUpdate won't fail
 * the application, but could lead to undefined behavior. Instead, we shall use method like
 * TaskSetManger.abort() to abort a stage and then fail the application (SPARK-31485).
 *
 * Delay Scheduling:
 * Delay scheduling is an optimization that sacrifices job fairness for data locality in order to
 * improve cluster and workload throughput. One useful definition of "delay" is how much time
 * has passed since the TaskSet was using its fair share of resources. Since it is impractical to
 * calculate this delay without a full simulation, the heuristic used is the time since the
 * TaskSetManager last launched a task and has not rejected any resources due to delay scheduling
 * since it was last offered its "fair share". A "fair share" offer is when [[resourceOffers]]'s
 * parameter "isAllFreeResources" is set to true. A "delay scheduling reject" is when a resource
 * is not utilized despite there being pending tasks (implemented inside [[TaskSetManager]]).
 * The legacy heuristic only measured the time since the [[TaskSetManager]] last launched a task,
 * and can be re-enabled by setting spark.locality.wait.legacyResetOnTaskLaunch to true.
 */
private[spark] class TaskSchedulerImpl(
                                        val sc: SparkContext,
                                        val maxTaskFailures: Int,
                                        isLocal: Boolean = false,
                                        clock: Clock = new SystemClock)
  extends TaskScheduler with Logging {

  import TaskSchedulerImpl._

  logInfo("=====3.成员变量包括：[taskId,executorId],[host,executor],rootPool,SchedulerBackend,DAGScheduler=====")
  logInfo("=====3.内容包括：FIFO/FAIR,Delay Scheduling,executor operations=====")

  def this(sc: SparkContext) = {
    this(sc, sc.conf.get(config.TASK_MAX_FAILURES))
  }

  // Lazily initializing healthTrackerOpt to avoid getting empty ExecutorAllocationClient,
  // because ExecutorAllocationClient is created after this TaskSchedulerImpl.
  private[scheduler] lazy val healthTrackerOpt = maybeCreateHealthTracker(sc)

  /**
   * Custom modifications by jaken
   *
   */
  val symbiosisExec = new HashSet[String]
  val conf = sc.conf

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL_MS = conf.get(SPECULATION_INTERVAL)

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  val MIN_TIME_TO_SPECULATION = 100

  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  val CPUS_PER_TASK = conf.get(config.CPUS_PER_TASK)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.  Protected by `this`
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // keyed by taskset
  // value is true if the task set has not rejected any resources due to locality
  // since the timer was last reset
  private val noRejectsSinceLastReset = new mutable.HashMap[TaskSet, Boolean]()
  private val legacyLocalityWaitReset = conf.get(LEGACY_LOCALITY_WAIT_RESET)

  // Protected by `this`
  private[scheduler] val taskIdToTaskSetManager = new ConcurrentHashMap[Long, TaskSetManager]
  // Protected by `this`
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  // We add executors here when we first get decommission notification for them. Executors can
  // continue to run even after being asked to decommission, but they will eventually exit.
  val executorsPendingDecommission = new HashMap[String, ExecutorDecommissionState]

  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size).toMap
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  // 主机上的executor的映射
  // 需要在taskset中使用 来计算节点之间的性能差异 取出其修饰 protected
  val hostToExecutors = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  // 需要在taskset中使用 来计算节点之间的性能差异 取出其修饰 protected
  val executorIdToHost = new HashMap[String, String]

  private val abortTimer = new Timer(true)
  // Exposed for testing
  // [未调度的任务集,过期的时间]
  val unschedulableTaskSetToExpiryTime = new HashMap[TaskSetManager, Long]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private var schedulableBuilder: SchedulableBuilder = null
  // default scheduler is FIFO
  logInfo("=====3.初始化默认的调度器为FIFO=====")
  private val schedulingModeConf = conf.get(SCHEDULER_MODE)
  val schedulingMode: SchedulingMode =
    try {
      SchedulingMode.withName(schedulingModeConf)
    } catch {
      case e: java.util.NoSuchElementException =>
        throw new SparkException(s"Unrecognized $SCHEDULER_MODE_PROPERTY: $schedulingModeConf")
    }

  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  private lazy val barrierSyncTimeout = conf.get(config.BARRIER_SYNC_TIMEOUT)

  private[scheduler] var barrierCoordinator: RpcEndpoint = null

  protected val defaultRackValue: Option[String] = None

  /**
   * Custom modifications by jaken
   * 根据host来统计对应的已经完成的任务/正在进行的任务，在每次任务调度前会更新一次
   */
/*
  private val hostCompleteTaskCount: mutable.Map[String, Int] = mutable.Map.empty
  private val recordTaskId: mutable.Set[Long] = mutable.Set.empty
*/

  private def maybeInitBarrierCoordinator(): Unit = {
    if (barrierCoordinator == null) {
      barrierCoordinator = new BarrierCoordinator(barrierSyncTimeout, sc.listenerBus,
        sc.env.rpcEnv)
      sc.env.rpcEnv.setupEndpoint("barrierSync", barrierCoordinator)
      logInfo("Registered BarrierCoordinator endpoint")
    }
  }

  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }

  // 点击initialize方法可以在SparkContext中找到使用的是哪种backend
  def initialize(backend: SchedulerBackend): Unit = {
    this.backend = backend
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
            s"$schedulingMode")
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start(): Unit = {
    // 也就是启动backend
    logInfo("=====3.TaskScheduler中启动SchedulerBackend=====")
    backend.start()

    if (!isLocal && conf.get(SPECULATION_ENABLED)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(
        () => Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        },
        SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook(): Unit = {
    waitBackendReady()
  }

  // 提交任务
  override def submitTasks(taskSet: TaskSet): Unit = {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks "
      + "resource profile " + taskSet.resourceProfileId)
    this.synchronized {
      // 创建TaskSetManager对象，其内部计算了每个任务的数据本地性等等
      logInfo(s"#####<开始>创建TaskSetManager#####")
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      logInfo(s"#####<结束>TaskSetManager=${manager}#####\n \n")
      val stage = taskSet.stageId
      // 得到(stageId,TaskSetManager)键值对
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])

      // Mark all the existing TaskSetManagers of this stage as zombie, as we are adding a new one.
      // This is necessary to handle a corner case. Let's say a stage has 10 partitions and has 2
      // TaskSetManagers: TSM1(zombie) and TSM2(active). TSM1 has a running task for partition 10
      // and it completes. TSM2 finishes tasks for partition 1-9, and thinks he is still active
      // because partition 10 is not completed yet. However, DAGScheduler gets task completion
      // events for all the 10 partitions and thinks the stage is finished. If it's a shuffle stage
      // and somehow it has missing map outputs, then DAGScheduler will resubmit it and create a
      // TSM3 for it. As a stage can't have more than one active task set managers, we must mark
      // TSM2 as zombie (it actually is).
      stageTaskSets.foreach { case (_, ts) =>
        // 设置taskSetManager为僵尸状态，也就是任务开始执行了
        ts.isZombie = true
      }
      stageTaskSets(taskSet.stageAttemptId) = manager
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run(): Unit = {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    // 上面都是初始化，最终是调用SchedulerBackend.reviveOffers
    // backend的类型取决于使用哪种master,查看本类中的initialize方法
    logInfo(s"####################################接收到提交任务需求,创建完TaskSetManager后,调用backend.reviveOffers()，进行任务调度###############################")
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
                                               taskSet: TaskSet,
                                               maxTaskFailures: Int): TaskSetManager = {
    // 创建TaskSetManager来管理任务集，其内部计算了每个任务的数据本地性
    new TaskSetManager(this, taskSet, maxTaskFailures, healthTrackerOpt, clock)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    // Kill all running tasks for the stage.
    killAllTaskAttempts(stageId, interruptThread, reason = "Stage cancelled")
    // Cancel all attempts for the stage.
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  override def killTaskAttempt(
                                taskId: Long,
                                interruptThread: Boolean,
                                reason: String): Boolean = synchronized {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
    if (execId.isDefined) {
      backend.killTask(taskId, execId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
    }
  }

  override def killAllTaskAttempts(
                                    stageId: Int,
                                    interruptThread: Boolean,
                                    reason: String): Unit = synchronized {
    logInfo(s"Killing all running tasks in stage $stageId: $reason")
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task.
        // 2. The task set manager has been created but no tasks have been scheduled. In this case,
        //    simply continue.
        tsm.runningTasksSet.foreach { tid =>
          taskIdToExecutorId.get(tid).foreach { execId =>
            backend.killTask(tid, execId, interruptThread, reason)
          }
        }
      }
    }
  }

  override def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
    taskResultGetter.enqueuePartitionCompletionNotification(stageId, partitionId)
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    noRejectsSinceLastReset -= manager.taskSet
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  /**
   * Offers resources to a single [[TaskSetManager]] at a given max allowed [[TaskLocality]].
   *
   * @param taskSet            task set manager to offer resources to
   * @param maxLocality        max locality to allow when scheduling
   * @param shuffledOffers     shuffled resource offers to use for scheduling,
   *                           remaining resources are tracked by below fields as tasks are scheduled
   * @param availableCpus      remaining cpus per offer,
   *                           value at index 'i' corresponds to shuffledOffers[i]
   * @param availableResources remaining resources per offer,
   *                           value at index 'i' corresponds to shuffledOffers[i]
   * @param tasks              tasks scheduled per offer, value at index 'i' corresponds to shuffledOffers[i]
   * @param addressesWithDescs tasks scheduler per host:port, used for barrier tasks
   * @return tuple of (no delay schedule rejects?, option of min locality of launched task)
   */
  private def resourceOfferSingleTaskSet(
                                          taskSet: TaskSetManager,
                                          maxLocality: TaskLocality,
                                          shuffledOffers: Seq[WorkerOffer],
                                          availableCpus: Array[Int],
                                          availableResources: Array[Map[String, Buffer[String]]],
                                          tasks: IndexedSeq[ArrayBuffer[TaskDescription]],
                                          addressesWithDescs: ArrayBuffer[(String, TaskDescription)])
  : (Boolean, Option[TaskLocality]) = {
    /*    logInfo(s"#####运行成功的taskInfo#####")
        for ((tid, info) <- taskSet.taskInfos if taskSet.successful(info.index)) {
          logInfo(s"#####tid=${tid},partitionId=${taskSet.tasks(info.index).partitionId},taskInfo=${info}#####")
        }*/
    /**
     * Custom modifications by jaken
     * 打印运行完成的任务信息 和 正在运行的任务信息 用来计算异构节点之间的性能差异
     */
    /*    logInfo(s"#####运行成功的taskInfo#####")
        for ((tid, successfulTaskInfo) <- taskSet.taskInfos if taskSet.successful(successfulTaskInfo.index)) {
          logInfo(s"#####successful tid=${tid},partitionId=${taskSet.tasks(successfulTaskInfo.index).partitionId},taskInfo=${successfulTaskInfo}#####")
        }
        logInfo(s"#####正在运行的taskInfo#####")
        for ((tid, runningTasInfo) <- taskSet.taskInfos if taskSet.runningTasksSet.contains(tid)) {
          logInfo(s"#####running tid=${tid},partitionId=${taskSet.tasks(runningTasInfo.index).partitionId},taskInfo=${runningTasInfo}#####")
        }*/
    // 筛选出正在运行或运行成功的任务的taskInfo,下面的方法在任务完成的时候可能会出错 因为任务从runnings集合出来了 但没有被加入到finish集合中
    /*    val hostCompleteTaskCount: mutable.Map[String, Int] = mutable.Map.empty
    for ((tid, taskInfo) <- taskSet.taskInfos if taskSet.successful(taskInfo.index) || taskSet.runningTasksSet.contains(tid)) {
      val host = taskInfo.host
      if(taskSet.successful(taskInfo.index)) logInfo(s"#####successful tid=${tid},partitionId=${taskSet.tasks(taskInfo.index).partitionId},host=${host}#####")
      else logInfo(s"#####running tid=${tid},partitionId=${taskSet.tasks(taskInfo.index).partitionId},host=${host}#####")
      if(!hostCompleteTaskCount.keys.exists(_ == host)) hostCompleteTaskCount.put(host,0)
      hostCompleteTaskCount(host) +=1
    }
    logInfo(s"#####hostCompleteTaskCount=${hostCompleteTaskCount}#####")*/

/*    // 过滤taskSet中已经完成或者正在运行的任务，如果有，对应host的value++
    for ((tid, taskInfo) <- taskSet.taskInfos if taskSet.runningTasksSet.contains(tid)) {
      val host = taskInfo.host
      logInfo(s"#####running tid=${tid},partitionId=${taskSet.tasks(taskInfo.index).partitionId},host=${host}#####")
      // 如果host没有出现过
      if(!hostCompleteTaskCount.keys.exists(_ == host)) hostCompleteTaskCount.put(host,0)
      if(!recordTaskId.contains(tid)) {
        hostCompleteTaskCount(host) +=1
        recordTaskId.add(tid)
      }
    }
    logInfo(s"#####hostCompleteTaskCount=${hostCompleteTaskCount}#####")*/

    var noDelayScheduleRejects = true
    var minLaunchedLocality: Option[TaskLocality] = None
    // nodes and executors that are excluded for the entire application have already been
    // filtered out by this point
    // i 表示的是executor的索引,遍历每个executor==================================
    //logInfo(s">>>>>四.<开始>遍历每个executor >>>>>")
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      // executor中有很多资源 , resourceProfileId表示资源的索引号
      val taskSetRpID = taskSet.taskSet.resourceProfileId
      // make the resource profile id a hard requirement for now - ie only put tasksets
      // on executors where resource profile exactly matches.
      if (taskSetRpID == shuffledOffers(i).resourceProfileId) {
        // 判断executor，能不能跑至少一个任务
        // 返回的是 Option[Map[资源名称:String, ResourceInformation]]
        // ResourceInformation为map[资源名称:String,资源对应的地址:Array[String]]
        // 返回的值是 满足了CPU要求之后，除了CPU以外的其他资源的分配方案

        // 如果CPU不满足要求,就返回None,自然不会将任务放置到当前executor中
        // TODO executor的可用CPU资源为0 也应当可以调度任务 在executor中定义一个属性 如果当ANY等级调度任务的时候
        val taskResAssignmentsOpt = resourcesMeetTaskRequirements(taskSet, availableCpus(i), availableResources(i))
        // 得到了资源名和对应的地址之后，挨个处理各种资源
        //logInfo(s"=====得到了taskResAssignmentsOpt:map[资源名称:String,map[资源名称:String,资源对应的地址:Array[String]]]:$taskResAssignmentsOpt=====")
        //logInfo(s"=====当前executor:$execId $host=====")
        //logInfo(s">>>>>五.<开始>遍历除CPU外的每种资源 >>>>>")
        if(taskResAssignmentsOpt.isDefined) logInfo(s"#####executor:${execId}满足CPU要求后的其他资源分配方案taskResAssignmentsOpt=${taskResAssignmentsOpt}#####")
        else logInfo(s"#####当前execId=${execId},host=${host},CPU资源不满足运行一个任务的最低要求,跳过#####")
        // 如果executor不满足CPU资源 taskResAssignmentsOpt没有定义 就不会进入下面的方法
        taskResAssignmentsOpt.foreach { taskResAssignments =>
          try {
            val prof = sc.resourceProfileManager.resourceProfileFromId(taskSetRpID)
            val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(prof, conf)
            // 返回的是执行的任务的详细信息 以及 有延迟调度被拒绝了?
            val (taskDescOption, didReject) = {
              // 给taskSet分配资源并执行============================================
              logInfo(s"=====在execID=${execId},host=${host}上,以${maxLocality}等级给stageId=${taskSet.stageId}分配${taskResAssignments}资源=====")
              // logInfo(s"=====进入resourceOffer方法，内部包含了延迟调度=====")
              // resourceOffer方法的作用就是 以maxLocality等级从任务队列中取<111111111111111111111111111>个任务放在execId上
              taskSet.resourceOffer(execId, host, maxLocality, taskResAssignments)
            }
            // 后面是一些信息的更新
            noDelayScheduleRejects &= !didReject
            // taskDescOption实际上只有一个任务
            for (task <- taskDescOption) {
              tasks(i) += task
              val tid = task.taskId
              val locality = taskSet.taskInfos(task.taskId).taskLocality
              minLaunchedLocality = minTaskLocality(minLaunchedLocality, Some(locality))
              taskIdToTaskSetManager.put(tid, taskSet)
              taskIdToExecutorId(tid) = execId
              executorIdToRunningTaskIds(execId).add(tid)
              /*START*/
              // TODO 这里可以拿到数据本地性 然后将executor标记为超线程
              val taskInfo = taskSet.taskInfos(task.taskId)
              if(locality == ANY) {
                taskInfo.forSymbiosis = true
                logInfo(s"#####当前任务等级为ANY,在host=${host},executor=${execId}上,计为共生任务1#####")
                symbiosisExec.add(execId)
              }

              // 如果是可共生任务 那么就不减
              if(!taskInfo.forSymbiosis) availableCpus(i) -= taskCpus
              /*END*/
              // 更新相应的资源
              // availableCpus(i) -= taskCpus
              assert(availableCpus(i) >= 0)
              task.resources.foreach { case (rName, rInfo) =>
                // Remove the first n elements from availableResources addresses, these removed
                // addresses are the same as that we allocated in taskResourceAssignments since it's
                // synchronized. We don't remove the exact addresses allocated because the current
                // approach produces the identical result with less time complexity.
                availableResources(i)(rName).remove(0, rInfo.addresses.size)
              }
              // Only update hosts for a barrier task.
              if (taskSet.isBarrier) {
                // The executor address is expected to be non empty.
                addressesWithDescs += (shuffledOffers(i).address.get -> task)
              }
            }
          } catch {
            case e: TaskNotSerializableException =>
              logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
              // Do not offer resources for this task, but don't throw an error to allow other
              // task sets to be submitted.
              return (noDelayScheduleRejects, minLaunchedLocality)
          }
        }
      }
    }
    (noDelayScheduleRejects, minLaunchedLocality)
  }

  /**
   * Check whether the resources from the WorkerOffer are enough to run at least one task.
   * Returns None if the resources don't meet the task requirements, otherwise returns
   * the task resource assignments to give to the next task. Note that the assignments maybe
   * be empty if no custom resources are used.
   */
  private def resourcesMeetTaskRequirements(
                                             taskSet: TaskSetManager,
                                             availCpus: Int,
                                             availWorkerResources: Map[String, Buffer[String]]
                                           ): Option[Map[String, ResourceInformation]] = {
    val rpId = taskSet.taskSet.resourceProfileId
    val taskSetProf = sc.resourceProfileManager.resourceProfileFromId(rpId)
    val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(taskSetProf, conf)
    // check if the ResourceProfile has cpus first since that is common case
    // 当前executor的CPU不够用 就返回None
    if (availCpus < taskCpus) return None
    // only look at the resource other then cpus
    // 只考虑除了CPU之外的其他资源 比如内存,GPU
    val tsResources = ResourceProfile.getCustomTaskResources(taskSetProf)
    if (tsResources.isEmpty) return Some(Map.empty)
    // 要返回的对象名称
    val localTaskReqAssign = HashMap[String, ResourceInformation]()
    // we go through all resources here so that we can make sure they match and also get what the
    // assignments are for the next task
    // 遍历任务需要的除CPU之外的资源
    for ((rName, taskReqs) <- tsResources) {
      val taskAmount = taskSetProf.getSchedulerTaskResourceAmount(rName)
      availWorkerResources.get(rName) match {
        case Some(workerRes) =>
          // 如果worker的资源能够满足当前任务的资源需求
          if (workerRes.size >= taskAmount) {
            // 将资源分配给任务
            localTaskReqAssign.put(rName, new ResourceInformation(rName,
              workerRes.take(taskAmount).toArray))
          } else {
            return None
          }
        case None => return None
      }
    }
    Some(localTaskReqAssign.toMap)
  }

  private def minTaskLocality(
                               l1: Option[TaskLocality],
                               l2: Option[TaskLocality]): Option[TaskLocality] = {
    if (l1.isEmpty) {
      l2
    } else if (l2.isEmpty) {
      l1
    } else if (l1.get < l2.get) {
      l1
    } else {
      l2
    }
  }

  /**
   * Called by cluster manager to offer resources on workers. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  // 返回的是所有任务集的任务描述信息
  def resourceOffers(
                      offers: IndexedSeq[WorkerOffer],
                      isAllFreeResources: Boolean = true): Seq[Seq[TaskDescription]] = synchronized {
    logInfo(s"##################<开始>任务调度入口,isAllFreeResources=${isAllFreeResources}#####################")
    // Mark each worker as alive and remember its hostname
    // Also track if new executor is added
    // 标记是不是有新创建的executor
    var newExecAvail = false
    // 遍历每一个executor，将新创建的executor记录在hostToExecutors和executorIdToRunningTaskIds中
    for (o <- offers) {
      // hostToExecutors是各个主机上运行的executor的映射[host,[e1,e2,e3...]]
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      // executorIdToRunningTaskIds = [executorId,[task1,task2,task3...]]
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        logInfo(s"=====executor:${o.executorId}是新创建的executor,维护executor相关信息=====")
        //logInfo(s"=====将executorId=${o.executorId}注册到executorIdToRunningTaskIds=${executorIdToRunningTaskIds}中=====")
        //logInfo(s"=====将[${o.host},${o.executorId}]注册到hostToExecutors=${hostToExecutors}中=====")
        hostToExecutors(o.host) += o.executorId
        //logInfo(s"=====告诉DAGScheduler[${o.host},${o.executorId}]被添加了=====")
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
        logInfo(s"#####hostToExecutors=${hostToExecutors}#####")
      }
    }

    // 取出所有的主机
    val hosts = offers.map(_.host).distinct
    // 计算host对应的rack信息
    for ((host, Some(rack)) <- hosts.zip(getRacksForHosts(hosts))) {
      hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += host
    }
    logInfo(s"=====更新hosts对应的rack信息：${hostsByRack}=====")


    // Before making any offers, include any nodes whose expireOnFailure timeout has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the excluded executors and nodes is only relevant when task offers are being made.
    healthTrackerOpt.foreach(_.applyExcludeOnFailureTimeout())

    // 过滤出能够正常运行的executor
    val filteredOffers = healthTrackerOpt.map { healthTracker =>
      offers.filter { offer =>
        !healthTracker.isNodeExcluded(offer.host) &&
          !healthTracker.isExecutorExcluded(offer.executorId)
      }
    }.getOrElse(offers)

    // 得到的是打乱顺序的可用的executor的资源信息
    val shuffledOffers = shuffleOffers(filteredOffers)
    if (offers.nonEmpty) {
      logInfo(s"=====将executor(workOffer)的顺序打乱=${shuffledOffers}=====")
    }

    // Build a list of tasks to assign to each worker.
    // Note the size estimate here might be off with different ResourceProfiles but should be
    // close estimate


    // tasks是 任务调度的结果矩阵==================================================================
    // tasks的列数 = executor的核心数 / 每个task消耗的核心数
    // tasks也就是将要放置在executor上的任务，是一个二维数组 每行表示executor[i]上分配的任务
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    // 后面的参数同tasks一样，长度都为executor的数目
    val availableResources = shuffledOffers.map(_.resources).toArray
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val availableExe = shuffledOffers.map(o => (o.executorId, o.host)).toArray
    logInfo(s"################各个executor信息：${availableExe.mkString("Array(", ", ", ")")}###################")
    logInfo(s"################各个executor可用CPU：${availableCpus.mkString("Array(", ", ", ")")}###################")
    logInfo(s"################各个executor可用资源：${availableResources.mkString("Array(", ", ", ")")}###################")
    val resourceProfileIds = shuffledOffers.map(o => o.resourceProfileId).toArray
    // 获取已排序的任务集的集合(TaskSetManager集合)，根据任务集的优先级进行排序，排序方法有FIFO和FAIR
    // 这里决定的是任务集调度的先后顺序
    val sortedTaskSets = rootPool.getSortedTaskSetQueue

    if (newExecAvail) {
      logInfo(s"#####<开始>由于存在新创建的executor，所以需要遍历每个taskSet,修改taskSetManager所记录的数据本地性等级#####")
    }
    for (taskSet <- sortedTaskSets) {
      //      logInfo(s"taskSet---stageId=${taskSet.stageId}" +
      //        s"runningTasksSet=${taskSet.runningTasksSet},taskInfos=${taskSet.taskInfos}," +
      //        s"myLocalityLevels=${taskSet.myLocalityLevels.mkString("Array(", ", ", ")")}")
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      // 如果创建了新的executor，就把executor放到taskSetManager中
      // 也就是说每一个taskSetManager中都记录了所有的executor的信息
      if (newExecAvail) {
        // 给任务集分配executor,其内部重新计算了数据本地性等级
        logInfo(s"=====<开始>重新计算taskSet=${taskSet}的数据本地性等级=====")
        taskSet.executorAdded()
        logInfo(s"=====<结束>重新计算taskSet=${taskSet}的数据本地性等级=====")
      }
    }
    if (sortedTaskSets.isEmpty) logInfo(s"################系统中不存在taskSet,无需更新数据本地性等级###################")
    if (newExecAvail) {
      logInfo(s"#####<结束>由于存在新创建的executor，所以需要遍历每个taskSet,修改taskSetManager所记录的数据本地性等级#####")
    }


    // Take each TaskSet in our scheduling order, and then offer it to each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    // 任务调度 ====================================
    //    logInfo(s">>>>>一.<开始>遍历每个任务集>>>>>")
    //    logInfo(s">>>>>二.<开始>遍历每个任务集的数据本地性等级myLocalityLevels:Array(NO_PREF, ANY)>>>>>")
    //    logInfo(s">>>>>三.<开始>如果有任务可以由当前等级调度，就一直用该等级，否则进入下一等级>>>>>")
    //    logInfo(s">>>>>四.<开始>遍历每个executor，一轮循环中，只在单个executor上最多放1个task >>>>>")
    //    logInfo(s">>>>>五.<开始>遍历除CPU外的每种资源，如果executor满足，则将任务放置>>>>>")

    for (taskSet <- sortedTaskSets) {
      // we only need to calculate available slots if using barrier scheduling, otherwise the
      // value is -1
      // 屏障调度??
      val numBarrierSlotsAvailable = if (taskSet.isBarrier) {
        logInfo(s"=====进入屏障调度相关配置=====")
        val rpId = taskSet.taskSet.resourceProfileId
        val availableResourcesAmount = availableResources.map { resourceMap =>
          // available addresses already takes into account if there are fractional
          // task resource requests
          resourceMap.map { case (name, addresses) => (name, addresses.length) }
        }
        calculateAvailableSlots(this, conf, rpId, resourceProfileIds, availableCpus,
          availableResourcesAmount)
      } else {
        -1
      }
      // Skip the barrier taskSet if the available slots are less than the number of pending tasks.
      // 如果屏障调度的任务集没有足够的资源 那么就跳过当前任务级
      if (taskSet.isBarrier && numBarrierSlotsAvailable < taskSet.numTasks) {
        // Skip the launch process.
        // TODO SPARK-24819 If the job requires more slots than available (both busy and free
        // slots), fail the job on submit.
        logInfo(s"Skip current round of resource offers for barrier stage ${taskSet.stageId} " +
          s"because the barrier taskSet requires ${taskSet.numTasks} slots, while the total " +
          s"number of available slots is $numBarrierSlotsAvailable.")
      } else {
        var launchedAnyTask = false
        var noDelaySchedulingRejects = true
        var globalMinLocality: Option[TaskLocality] = None
        // Record all the executor IDs assigned barrier tasks on.
        // TaskDescription中记录了将task放置在哪个executor上
        val addressesWithDescs = ArrayBuffer[(String, TaskDescription)]()
        // 遍历每个任务集记录的数据本地性等级=============================
        // myLocalityLevels总是最大的数据本地性等级排到最前面
        //logInfo(s"=====当前taskSet=${taskSet}=====")
        //logInfo(s">>>>>二.<开始>遍历每个任务集的数据本地性等级myLocalityLevels:${taskSet.myLocalityLevels.mkString("Array(", ", ", ")")}>>>>>")
        for (currentMaxLocality <- taskSet.myLocalityLevels) {
          // 标记能否以当前的最大数据本地性等级运行任务
          var launchedTaskAtCurrentMaxLocality = false
          // 这里的循环意思是：如果能够以当前的currentMaxLocality等级运行至少一个任务，就会继续以currentMaxLocality运行
          // 反之跳出循环,以下一等级调度任务
          do {
            logInfo(s"#################################以${currentMaxLocality}等级调度任务#####################################")
            // 真正开始调度任务集中的单个任务集，返回值为(调度是否被拒绝 , 任务集中最小的数据本地性等级)
            // 关键点minLocality，决定了是否退出循环
            // resourceOfferSingleTaskSet() 内部遍历所有executor,只在executor上最多放1个task
            val (noDelayScheduleReject, minLocality) = resourceOfferSingleTaskSet(
              taskSet, currentMaxLocality, shuffledOffers, availableCpus,
              availableResources, tasks, addressesWithDescs)
            // minLocality没有定义，就说明resourceOfferSingleTaskSet()中没有task被放置
            launchedTaskAtCurrentMaxLocality = minLocality.isDefined
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
            /*这里是很重要的 也就是说 只要noDelaySchedulingRejects曾经为false 它就一直为false了
            * 对应的情况就是 如果有 没有运行新任务 & 数据本地性等级=ANY & 队列中存在任务,说明此次任务调度被拒了,没有使用任何资源
            * e.g. 某个节点中没有任务了 那次调度肯定没有使用任何资源 那么noDelaySchedulingRejects从那之后一直为false
            * */
            noDelaySchedulingRejects &= noDelayScheduleReject
            globalMinLocality = minTaskLocality(globalMinLocality, minLocality)
          } while (launchedTaskAtCurrentMaxLocality)
        }
        // legacyLocalityWaitReset默认是false 也就是使用新版的重置机制：不是在每个任务调度结束就重置时间
        if (!legacyLocalityWaitReset) {
          // 延迟调度没有拒绝资源，也就是任务正常调度了
          if (noDelaySchedulingRejects) {
            if (launchedAnyTask &&
              // noRejectsSinceLastReset表示该taskSet 现在本轮处于调度拒绝的状态 只有当执行了一次isAllFreeResources=true的调度(该调度就是延迟调度的应用)之后 才会恢复ture
              (isAllFreeResources || noRejectsSinceLastReset.getOrElse(taskSet.taskSet, true))) {
              // 需要重置延迟调度的时间
              taskSet.resetDelayScheduleTimer(globalMinLocality)
              logInfo(s"=====重置延迟调度时间,noDelaySchedulingRejects=true,=====")
              noRejectsSinceLastReset.update(taskSet.taskSet, true)
            }
          }
          // 有资源运行任务 数据队列不为空 但是却没有任务调度
          else {
            logInfo(s"=====noDelaySchedulingRejects=false,没有使用任何资源,不重置延迟调度的时间=====")
            noRejectsSinceLastReset.update(taskSet.taskSet, false)
          }
        }
        // 没有任务运行
        if (!launchedAnyTask) {
          // 检查任务集是不是完全不能被执行，只有当任务集中的所有任务都不能被调度的时候，才认为这个任务集被排除了
          // 返回任务集中第一个不能被调度的任务的index
          taskSet.getCompletelyExcludedTaskIfAny(hostToExecutors)
            // 实际上只有一个taskIndex
            .foreach { taskIndex =>

              /** 当taskSet没有被调度的时候，和动态资源调度相关==========================================
               * 如果任务集是不可调度的，我们会尝试找到一个空闲的被排除的执行器(也就是不能参与任务调度的执行器)，并杀死这个空闲的执行器，
               * 同时启动一个终止计时器，如果任务集在一定时间内还是没有被调度，
               * 那么说明我们无法调度任务集中的任何任务，就会丢弃任务集。
               * If the taskSet is unschedulable we try to find an existing idle excluded
               * executor and kill the idle executor and kick off an abortTimer which if it doesn't
               * schedule a task within the the timeout will abort the taskSet if we were unable to
               * schedule any task from the taskSet.
               * 注1：我们在每个任务集的基础上而不是在每个任务的基础上跟踪调度能力。
               * Note 1: We keep track of schedulability on a per taskSet basis rather than on a per
               * task basis.
               * 注2：当有超过一个空闲的被排除的执行者并且动态分配开启时，任务集仍然可以被中止。
               * Note 2: The taskSet can still be aborted when there are more than one idle
               * excluded executors and dynamic allocation is on.
               * 这可能发生在被杀死的空闲执行器没有被ExecutorAllocationManager及时替换的情况下，
               * 因为它依赖于挂起的任务，不会在空闲超时时杀死执行器，导致中止计时器过期并中止任务集。
               * This can happen when a killed
               * idle executor isn't replaced in time by ExecutorAllocationManager as it relies on
               * pending tasks and doesn't kill executors on idle timeouts, resulting in the abort
               * timer to expire and abort the taskSet.
               * 如果没有空闲的执行器，并且动态分配被启用，那么我们会通知ExecutorAllocationManager
               * 分配更多的执行器来调度不可调度的任务，否则我们将立即中止。
               * If there are no idle executors and dynamic allocation is enabled, then we would
               * notify ExecutorAllocationManager to allocate more executors to schedule the
               * unschedulable tasks else we will abort immediately.
               */

              executorIdToRunningTaskIds.find(x => !isExecutorBusy(x._1)) match {
                // 如果存在空闲的executor
                case Some((executorId, _)) =>
                  // 如果设置有过期时间的任务集里 没有包含这个未被调度的任务集
                  if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                    // 删除空闲的executor
                    healthTrackerOpt.foreach(blt => blt.killExcludedIdleExecutor(executorId))
                    // 将该不能调度的TaskSet,放到等待放弃的TaskSet集合中，并设置放弃的时间
                    updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                  }
                // executor都不空闲的时候
                case None =>
                  //  Notify ExecutorAllocationManager about the unschedulable task set,
                  // in order to provision more executors to make them schedulable
                  // 如果动态资源调度开启了
                  if (Utils.isDynamicAllocationEnabled(conf)) {
                    // 如果设置有过期时间的任务集里 没有包含这个未被调度的任务集
                    if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                      // 通知ExecutorAllocationManager去申请更多的executor
                      logInfo("Notifying ExecutorAllocationManager to allocate more executors to" +
                        " schedule the unschedulable task before aborting" +
                        " stage ${taskSet.stageId}.")
                      dagScheduler.unschedulableTaskSetAdded(taskSet.taskSet.stageId,
                        taskSet.taskSet.stageAttemptId)
                      // 将该不能调度的TaskSet,放到等待放弃的TaskSet集合中，并设置放弃的时间
                      updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                    }
                  }
                  // 如果动态资源调度关闭了，但是executor依然忙碌，没有办法只能丢弃任务集了
                  else {
                    // Abort Immediately
                    logInfo("Cannot schedule any task because all executors excluded from " +
                      "failures. No idle executors can be found to kill. Aborting stage " +
                      s"${taskSet.stageId}.")
                    taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)
                  }
              }
            }
        }

        /**
         * 只要任务集中有任务运行
         * 那就不能放弃该任务集, 因为随时可能出现  不被排除的executor(可用的executor)
         * 只要我们有一个不被排除的执行器，可以用来安排任何活动任务集的任务，
         * 我们就想推迟杀死**任何**任务集。这就保证了工作能够取得进展。
         * 注意：理论上有可能一个任务集永远不会被安排在一个非排除的执行器上，
         * 并且中止计时器不会因为不断提交新的任务集而启动。更多细节请参见PR。
         * */
        else {
          // We want to defer killing any taskSets as long as we have a non excluded executor
          // which can be used to schedule a task from any active taskSets. This ensures that the
          // job can make progress.
          // Note: It is theoretically possible that a taskSet never gets scheduled on a
          // non-excluded executor and the abort timer doesn't kick in because of a constant
          // submission of new TaskSets. See the PR for more details.
          if (unschedulableTaskSetToExpiryTime.nonEmpty) {
            logInfo("Clearing the expiry times for all unschedulable taskSets as a task was " +
              "recently scheduled.")
            // Notify ExecutorAllocationManager as well as other subscribers that a task now
            // recently becomes schedulable
            dagScheduler.unschedulableTaskSetRemoved(taskSet.taskSet.stageId,
              taskSet.taskSet.stageAttemptId)
            // 只要有一个任务被执行了 就会清空所有的 未被调度的有过期时间的任务集
            unschedulableTaskSetToExpiryTime.clear()
          }
        }

        if (launchedAnyTask && taskSet.isBarrier) {
          // Check whether the barrier tasks are partially launched.
          // TODO SPARK-24818 handle the assert failure case (that can happen when some locality
          // requirements are not fulfilled, and we should revert the launched tasks).
          if (addressesWithDescs.size != taskSet.numTasks) {
            val errorMsg =
              s"Fail resource offers for barrier stage ${taskSet.stageId} because only " +
                s"${addressesWithDescs.size} out of a total number of ${taskSet.numTasks}" +
                s" tasks got resource offers. This happens because barrier execution currently " +
                s"does not work gracefully with delay scheduling. We highly recommend you to " +
                s"disable delay scheduling by setting spark.locality.wait=0 as a workaround if " +
                s"you see this error frequently."
            logWarning(errorMsg)
            taskSet.abort(errorMsg)
            throw new SparkException(errorMsg)
          }

          // materialize the barrier coordinator.
          maybeInitBarrierCoordinator()

          // Update the taskInfos into all the barrier task properties.
          val addressesStr = addressesWithDescs
            // Addresses ordered by partitionId
            .sortBy(_._2.partitionId)
            .map(_._1)
            .mkString(",")
          addressesWithDescs.foreach(_._2.properties.setProperty("addresses", addressesStr))

          logInfo(s"Successfully scheduled all the ${addressesWithDescs.size} tasks for barrier " +
            s"stage ${taskSet.stageId}.")
        }
      }
    }
    logInfo(s"##################<结束>任务调度出口,isAllFreeResources=${isAllFreeResources}#####################\n")
    // TODO SPARK-24823 Cancel a job that contains barrier stage(s) if the barrier tasks don't get
    // launched within a configured time.
    if (tasks.nonEmpty) {
      hasLaunchedTask = true
    }
    return tasks.map(_.toSeq)
  }

  // 将taskSet放到[ 未调度的任务集 ,  过期时间 ]中
  private def updateUnschedulableTaskSetTimeoutAndStartAbortTimer(
                                                                   taskSet: TaskSetManager,
                                                                   taskIndex: Int): Unit = {
    // 任务集未调度的时间超过UNSCHEDULABLE_TASKSET_TIMEOUT(120s)时 就会放弃该任务集
    val timeout = conf.get(config.UNSCHEDULABLE_TASKSET_TIMEOUT) * 1000
    // 当前时间 + 未调度的期限(120s) = 任务集过期的时间
    unschedulableTaskSetToExpiryTime(taskSet) = clock.getTimeMillis() + timeout
    logInfo(s"Waiting for $timeout ms for completely " +
      s"excluded task to be schedulable again before aborting stage ${taskSet.stageId}.")
    abortTimer.schedule(
      createUnschedulableTaskSetAbortTimer(taskSet, taskIndex), timeout)
  }

  private def createUnschedulableTaskSetAbortTimer(
                                                    taskSet: TaskSetManager,
                                                    taskIndex: Int): TimerTask = {
    new TimerTask() {
      override def run(): Unit = TaskSchedulerImpl.this.synchronized {
        if (unschedulableTaskSetToExpiryTime.contains(taskSet) &&
          // 如果当前时间已经达到了任务集的放弃时间 就放弃任务集(stage)
          unschedulableTaskSetToExpiryTime(taskSet) <= clock.getTimeMillis()) {
          logInfo("Cannot schedule any task because all executors excluded due to failures. " +
            s"Wait time for scheduling expired. Aborting stage ${taskSet.stageId}.")
          taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)
        } else {
          this.cancel()
        }
      }
    }
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        Option(taskIdToTaskSetManager.get(tid)) match {
          case Some(taskSet) =>
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, {
                val errorMsg =
                  "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"
                taskSet.abort(errorMsg)
                throw new SparkException(errorMsg)
              })
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  ExecutorProcessLost(
                    s"Task $tid was lost, so marking the executor as lost as well."))
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            // 如果任务为完成状态
            if (TaskState.isFinished(state)) {
              cleanupTaskState(tid)
              taskSet.removeRunningTask(tid)
              // 任务是完成状态 就将成功的任务从队列中移除
              if (state == TaskState.FINISHED) {
                /**
                 * Custom modifications by jaken
                 * 更新任务的完成时间 该时间不包括拉取任务结果数据的时间
                 */
                val time = clock.getTimeMillis()
                val info = taskSet.taskInfos(tid)
                info.finishTimeWithoutFetch = time
                info.durationWithoutFetch = time - info.launchTime
                logInfo(s"=====更新已完成任务tid=${tid},finishTimeWithoutFetch=${time},durationWithoutFetch=${info.durationWithoutFetch}=====")
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and executor metrics, and let the master know that the
   * BlockManager is still alive. Return true if the driver knows about the given block manager.
   * Otherwise, return false, indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
                                          execId: String,
                                          accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
                                          blockManagerId: BlockManagerId,
                                          executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        Option(taskIdToTaskSetManager.get(id)).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId,
      executorUpdates)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
                            taskSetManager: TaskSetManager,
                            tid: Long,
                            taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
                        taskSetManager: TaskSetManager,
                        tid: Long,
                        taskState: TaskState,
                        reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  /**
   * Marks the task has completed in the active TaskSetManager for the given stage.
   *
   * After stage failure and retry, there may be multiple TaskSetManagers for the stage.
   * If an earlier zombie attempt of a stage completes a task, we can ask the later active attempt
   * to skip submitting and running the task for the same partition, to save resource. That also
   * means that a task completion from an earlier zombie attempt can lead to the entire stage
   * getting marked as successful.
   */
  private[scheduler] def handlePartitionCompleted(stageId: Int, partitionId: Int) = synchronized {
    taskSetsByStageIdAndAttempt.get(stageId).foreach(_.values.filter(!_.isZombie).foreach { tsm =>
      tsm.markPartitionCompleted(partitionId)
    })
  }

  def error(message: String): Unit = {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop(): Unit = {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    if (barrierCoordinator != null) {
      barrierCoordinator.stop()
    }
    starvationTimer.cancel()
    abortTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks(): Unit = {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorDecommission(
                                     executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit = {
    synchronized {
      // Don't bother noting decommissioning for executors that we don't know about
      if (executorIdToHost.contains(executorId)) {
        executorsPendingDecommission(executorId) =
          ExecutorDecommissionState(clock.getTimeMillis(), decommissionInfo.workerHost)
      }
    }
    rootPool.executorDecommission(executorId)
    backend.reviveOffers()
  }

  override def getExecutorDecommissionState(executorId: String)
  : Option[ExecutorDecommissionState] = synchronized {
    logInfo(s"#####executorId=${executorId},executorsPendingDecommission=${executorsPendingDecommission},executorsPendingDecommission.get(executorId)=${executorsPendingDecommission.get(executorId)}#####")
    executorsPendingDecommission.get(executorId)
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the worker while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo(s"Handle removed worker $workerId: $message")
    dagScheduler.workerRemoved(workerId, host, message)
  }

  private def logExecutorLoss(
                               executorId: String,
                               hostPort: String,
                               reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach {
        _.remove(tid)
      }
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    executorsPendingDecommission.remove(executorId)

    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    healthTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  def executorAdded(execId: String, host: String): Unit = {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.filterNot(isExecutorDecommissioned)).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    logInfo(s"#####hostToExecutors=${hostToExecutors}#####")
    val res = hostToExecutors.get(host)
      .exists(executors => executors.exists(e => !isExecutorDecommissioned(e)))
    logInfo(s"=====进入hasExecutorsAliveOnHost()方法,host=${host},hostToExecutors.get(host)=${hostToExecutors.get(host)},executors.exists(e => !isExecutorDecommissioned(e))=${res}=====")
    res
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.get(rack)
      .exists(hosts => hosts.exists(h => !isHostDecommissioned(h)))
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId) && !isExecutorDecommissioned(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  // exposed for test
  protected final def isExecutorDecommissioned(execId: String): Boolean = {
    logInfo(s"#####execId=${execId},getExecutorDecommissionState(execId)=${getExecutorDecommissionState(execId)}#####")
    getExecutorDecommissionState(execId).isDefined
  }

  // exposed for test
  protected final def isHostDecommissioned(host: String): Boolean = {
    hostToExecutors.get(host).exists { executors =>
      executors.exists(e => getExecutorDecommissionState(e).exists(_.workerHost.isDefined))
    }
  }

  /**
   * Get a snapshot of the currently excluded nodes for the entire application. This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def excludedNodes(): Set[String] = {
    healthTrackerOpt.map(_.excludedNodeList()).getOrElse(Set.empty)
  }

  /**
   * Get the rack for one host.
   *
   * Note that [[getRacksForHosts]] should be preferred when possible as that can be much
   * more efficient.
   */
  def getRackForHost(host: String): Option[String] = {
    getRacksForHosts(Seq(host)).head
  }

  /**
   * Get racks for multiple hosts.
   *
   * The returned Sequence will be the same length as the hosts argument and can be zipped
   * together with the hosts argument.
   */
  def getRacksForHosts(hosts: Seq[String]): Seq[Option[String]] = {
    hosts.map(_ => defaultRackValue)
  }

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  // exposed for testing
  private[scheduler] def taskSetManagerForAttempt(
                                                   stageId: Int,
                                                   stageAttemptId: Int): Option[TaskSetManager] = synchronized {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }
}


private[spark] object TaskSchedulerImpl {

  val SCHEDULER_MODE_PROPERTY = SCHEDULER_MODE.key

  /**
   * Calculate the max available task slots given the `availableCpus` and `availableResources`
   * from a collection of ResourceProfiles. And only those ResourceProfiles who has the
   * same id with the `rpId` can be used to calculate the task slots.
   *
   * @param scheduler          the TaskSchedulerImpl instance
   * @param conf               SparkConf used to calculate the limiting resource and get the cpu amount per task
   * @param rpId               the target ResourceProfile id. Only those ResourceProfiles who has the same id
   *                           with it can be used to calculate the task slots.
   * @param availableRPIds     an Array of ids of the available ResourceProfiles from the executors.
   * @param availableCpus      an Array of the amount of available cpus from the executors.
   * @param availableResources an Array of the resources map from the executors. In the resource
   *                           map, it maps from the resource name to its amount.
   * @return the number of max task slots
   */
  def calculateAvailableSlots(
                               scheduler: TaskSchedulerImpl,
                               conf: SparkConf,
                               rpId: Int,
                               availableRPIds: Array[Int],
                               availableCpus: Array[Int],
                               availableResources: Array[Map[String, Int]]): Int = {
    val resourceProfile = scheduler.sc.resourceProfileManager.resourceProfileFromId(rpId)
    val coresKnown = resourceProfile.isCoresLimitKnown
    val (limitingResource, limitedByCpu) = {
      val limiting = resourceProfile.limitingResource(conf)
      // if limiting resource is empty then we have no other resources, so it has to be CPU
      if (limiting == ResourceProfile.CPUS || limiting.isEmpty) {
        (ResourceProfile.CPUS, true)
      } else {
        (limiting, false)
      }
    }
    val cpusPerTask = ResourceProfile.getTaskCpusOrDefaultForProfile(resourceProfile, conf)
    val taskLimit = resourceProfile.taskResources.get(limitingResource).map(_.amount).get

    availableCpus.zip(availableResources).zip(availableRPIds)
      .filter { case (_, id) => id == rpId }
      .map { case ((cpu, resources), _) =>
        val numTasksPerExecCores = cpu / cpusPerTask
        if (limitedByCpu) {
          numTasksPerExecCores
        } else {
          val availAddrs = resources.getOrElse(limitingResource, 0)
          val resourceLimit = (availAddrs / taskLimit).toInt
          // when executor cores config isn't set, we can't calculate the real limiting resource
          // and number of tasks per executor ahead of time, so calculate it now based on what
          // is available.
          if (!coresKnown && numTasksPerExecCores <= resourceLimit) {
            numTasksPerExecCores
          } else {
            resourceLimit
          }
        }
      }.sum
  }

  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used. The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given {@literal <h1, [o1, o2, o3]>}, {@literal <h2, [o4]>} and
   * {@literal <h3, [o5, o6]>}, returns {@literal [o1, o5, o4, o2, o6, o3]}.
   */
  def prioritizeContainers[K, T](map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  private def maybeCreateHealthTracker(sc: SparkContext): Option[HealthTracker] = {
    if (HealthTracker.isExcludeOnFailureEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend match {
        case b: ExecutorAllocationClient => Some(b)
        case _ => None
      }
      Some(new HealthTracker(sc, executorAllocClient))
    } else {
      None
    }
  }

}
