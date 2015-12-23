package StreamIng.Util

import MyUtil.RddGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

object GenerateQueueRDD {
  def generateQueueRDD(sc: StreamingContext): mutable.SynchronizedQueue[RDD[Int]] ={
    val sparkContext = sc.sparkContext
    val queue = new mutable.SynchronizedQueue[RDD[Int]]()

    (0 to 10).foreach(i => queue.enqueue(RddGenerator.GenerateNumberRDD(sparkContext)))

    queue


  }
}
