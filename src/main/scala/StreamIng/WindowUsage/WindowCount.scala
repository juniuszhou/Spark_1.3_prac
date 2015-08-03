package main.scala.StreamIng.WindowUsage

import main.scala.StreamIng.StreamingFirst
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ConstantInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by junius on 15-7-20.
 */
object WindowCount {
  def printOutputRDD(count: DStream[Int]){
    count.foreachRDD(rdd =>  { rdd.foreach(u => { print(u.toString + " ")

        }
      )
      println()
      println(" rdd split _________________")


    })
  }

  def main (args: Array[String]) {
    val sparkConf = new SparkConf()

      sparkConf.setAppName("count ")
      sparkConf.setMaster("local[4]")

    val sc = new StreamingContext(sparkConf, Seconds(2))
    val listData = List(0,1,2,3,9)
    // it is ok. but you can't cal sparkContext.parallelize directly. why?????
    val rdd = sc.sparkContext.parallelize[Int](listData, 4)
    val input = new ConstantInputDStream[Int](sc, rdd)

    val window = input.window(Seconds(10), Seconds(10))
    printOutputRDD(window)
    sc.start()
    sc.awaitTermination()
  }
}


