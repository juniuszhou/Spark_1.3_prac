package StreamIng.KafkaSource

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by junius on 9/25/15.
 */
object Second {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]")setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // lpasSyntheticShops  uis
    val topicName = "lpasSyntheticShops"
    val topicMap = Map(topicName -> 4)
    val group = "beijing"
    //val zkQuorum = "zk1.us-east-1.prod.expedia.com:2181,zk2.us-east-1.prod.expedia.com:2181,zk3.us-east-1.prod.expedia.com:2181"
    val zkQuorum = "zk1.us-east-1.test.expedia.com:2181/kafka/ewep"
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.foreachRDD(rdd => {
      rdd.foreach(println)
      println("***********")
    })
    println(lines.count())
    println("***********")

    ssc.start()
    ssc.awaitTermination()
  }
}
