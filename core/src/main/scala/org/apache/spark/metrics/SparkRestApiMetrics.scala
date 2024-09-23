package org.apache.spark.metrics

import java.net.{HttpURLConnection, URL}
import scala.io.Source
import play.api.libs.json._ // Assuming you're using Play JSON for parsing

object SparkRestApiMetrics {

  // Function to send GET request to the REST API
  def getTaskMetrics(appId: String, stageId: Int): Seq[TaskInfo] = {
    // Construct the API URL for the task metrics
    // http://c04xy:18080/api/v1/applications/app-20240923152630-0005/stages/1/0/taskList?offset=0&length=200
    val url = new URL(s"http://c04xy:18080/api/v1/applications/$appId/stages/$stageId/0/taskList")
    val startTime = System.currentTimeMillis()
    // Open a connection to the URL
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")

    // Check the response code
    val responseCode = connection.getResponseCode
    if (responseCode == 200) {
      // Read the response from the API
      val responseStream = Source.fromInputStream(connection.getInputStream).mkString
      connection.disconnect()

      // Parse the JSON response (using Play JSON in this example)
      val json: JsValue = Json.parse(responseStream)
      val taskInfoList: Seq[TaskInfo] = json.as[Seq[TaskInfo]]
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime
      println(s"API 请求耗时: $duration 毫秒")
      // 输出任务信息
/*      taskInfoList.foreach { taskInfo =>
        println(s"Task ID: ${taskInfo.taskId}, Executor ID: ${taskInfo.executorId}, Status: ${taskInfo.status}")
        println(s"Executor Run Time: ${taskInfo.taskMetrics.executorRunTime}")
      }*/
      taskInfoList
    } else {
      println(s"Failed to fetch task metrics. HTTP response code: $responseCode")
      Seq.empty
    }
  }

  def main(args: Array[String]): Unit = {
    val appId = "app-20240923152630-0005"
    val stageId = 0  // Replace with your actual stage ID
    val startTime = System.currentTimeMillis()
    getTaskMetrics(appId, stageId)
    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime
    println(s"API 请求耗时: $duration 毫秒")
  }
}

