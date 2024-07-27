package org.apache.spark.examples
import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]): Unit = {
    // 创建一个包含一些整数的ArrayBuffer
    val arrayBuffer = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9)

    // 在下标为6的位置插入一个元素，比如插入元素10
    val elementToInsert = 10
    val insertionIndex = 9

    arrayBuffer.remove(8)

    // 打印插入元素后的ArrayBuffer
    println(arrayBuffer)

  }
}
