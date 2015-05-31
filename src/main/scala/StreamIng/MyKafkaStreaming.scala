package main.scala.StreamIng

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by junius on 15-5-31.
 */
object MyKafkaStreaming {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("count ")
      .setMaster("local[4]")

    val sc = new StreamingContext(sparkConf, Seconds(10))

    // val stream = KafkaUtils.createStream(sc,
  }
}
