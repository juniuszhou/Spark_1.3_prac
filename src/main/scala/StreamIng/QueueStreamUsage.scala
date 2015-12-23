package StreamIng

import StreamIng.Util.GenerateQueueRDD
import MyUtil.SetLogLevel
import _root_.MyUtil.SetLogLevel
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by junius on 10/8/15.
 */
object QueueStreamUsage {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("count ")
      .setMaster("local[4]")

    val sc = new StreamingContext(sparkConf, Seconds(10))
    //sc.queueStream()
    SetLogLevel.setLogLevels()
    val data = sc.queueStream(GenerateQueueRDD.generateQueueRDD(sc))

    data.foreachRDD(rdd => rdd.foreach(println))
    sc.start()
    sc.awaitTermination()
  }
}
