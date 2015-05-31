package main.scala.StreamIng

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.Logger

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */

object StreamUnion {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("count ")
      .setMaster("local[4]")
      .set("spark.conf.dir", "/home/junius/my_git/Spark_1.3_prac/conf/")

    val sc = new StreamingContext(sparkConf, Seconds(10))
    // sparkConf
    // Logger.
    val port : Int = 9999
    val port2: Int = 9998
    // socketTextStream just connect to this port and then get data.
    //So you must must must run nc -lk 9999 before run this program.
    val lines = sc.socketTextStream("localhost", port, StorageLevel.MEMORY_ONLY_SER).cache

    val lines2 = sc.socketTextStream("localhost", port2, StorageLevel.MEMORY_ONLY_SER).cache

    val word = lines.map(str => str.split("a"))
    val words = lines2.map(str => str.split("a"))

    //lines.foreachRDD(rdd => rdd.foreach(println))
    //lines2.foreachRDD(rdd => rdd.foreach(println))

    val union = word.union(words).foreachRDD(rdd => rdd.foreach(arr => arr.foreach(println)))

    sc.start()
    sc.awaitTermination()

  }
}
