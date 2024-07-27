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

package org.apache.spark

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.{Millis, Span}

import scala.util.Random

class UnpersistSuite extends SparkFunSuite with LocalSparkContext with TimeLimits {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  test("unpersist RDD") {
    sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    rdd.count
    assert(sc.persistentRdds.nonEmpty)
    rdd.unpersist(blocking = true)
    assert(sc.persistentRdds.isEmpty)

    failAfter(Span(3000, Millis)) {
      try {
        while (! sc.getRDDStorageInfo.isEmpty) {
          Thread.sleep(200)
        }
      } catch {
        case _: Throwable => Thread.sleep(10)
          // Do nothing. We might see exceptions because block manager
          // is racing this thread to remove entries from the driver.
      }
    }
    assert(sc.getRDDStorageInfo.isEmpty)
  }
  test("random") {
    val random = new Random()

    // 生成 1000 个随机数
    val randomValues = Array.fill(1000)(random.nextDouble())

    // 检查分布情况
    val countInRanges = randomValues.groupBy {
      case x if x < 0.1 => "0.0-0.1"
      case x if x < 0.2 => "0.1-0.2"
      case x if x < 0.3 => "0.2-0.3"
      case x if x < 0.4 => "0.3-0.4"
      case x if x < 0.5 => "0.4-0.5"
      case x if x < 0.6 => "0.5-0.6"
      case x if x < 0.7 => "0.6-0.7"
      case x if x < 0.8 => "0.7-0.8"
      case x if x < 0.9 => "0.8-0.9"
      case x if x < 1.0 => "0.9-1.0"
    }.mapValues(_.length).toMap

    // 输出每个区间内的随机数数量
    countInRanges.toSeq.sortBy(_._1).foreach { case (range, count) =>
      println(s"$range: $count")
    }
  }

}
