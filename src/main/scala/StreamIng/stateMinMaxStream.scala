package main.scala.StreamIng

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming._


object stateMinMaxStream {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("count ")
      .setMaster("local[4]")

    Logger.getRootLogger.setLevel(Level.WARN)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    var min = Long.MaxValue
    var max = Long.MinValue

    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    val nums = lines.flatMap(_.split(",")).map(_.toLong)

    // nums.foreachRDD(rdd => rdd.foreach(println))

    nums.reduce((l1, l2) => Math.min(l1, l2)).foreachRDD(rdd => {
      rdd.foreach(l => {
        min = Math.min(min, l)
        println("%%%%%%%%%%%%%%%%%%%%%%")
        println("min is ", min,"l is",l)
      })

    })

    nums.reduce((l1, l2) => Math.max(l1, l2)).foreachRDD(rdd => {
      rdd.foreach(l => {
        max = Math.max(max, l)
        println("%%%%%%%%%%%%%%%%%%%%%%")
        println("max is ", max, "l is",l)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}