package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

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

object StreamingFirst {
  
  def printOutputRDD(count: DStream[(String, Int)]){
    count.foreachRDD(rdd =>  { rdd.map(u => println( u._1 + " " + u._2))
                               println(" rdd split _________________")
      
    })    
  }
  
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("count ")
    							   .setMaster("local[4]")
    
    val sc = new StreamingContext(sparkConf, Seconds(10))
    val port : Int = 9999
    // socketTextStream just connect to this port and then get data.
    //So you must must must run nc -lk 9999 before run this program.
    val lines = sc.socketTextStream("localhost", port, StorageLevel.MEMORY_ONLY_SER).cache

    val words = lines.flatMap(_.split(" "))
    val counts  = 0 // words.map(x => (x, 1)).reduceByKey(_ + _)
   // printOutputRDD(counts)
    //counts.print()
    sc.start()
    sc.awaitTermination()

    // you can set as false then spark context still active.
    // it is useful for instance other task like graph or machine learning running.
    sc.stop(stopSparkContext = false)
  }
}