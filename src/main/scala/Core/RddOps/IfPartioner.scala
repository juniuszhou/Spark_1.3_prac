package main.scala.Core.RddOps

import main.scala.RddGenerator
import org.apache.spark.SparkContext

/**
 * Created by junius on 15-8-10.
 */
object IfPartioner {
  def main (args: Array[String]) {
    val sc = new SparkContext("local", "Simple App")
    val rdd =  RddGenerator2.GeneratePairRDD(sc)

    // for rdd generated from collections. there are several partitions for
    // parallelism, but there is no partitioner. partitioner can put the data with
    // the same value via hash or range into same partition. then some local operation
    // can be done otherwise need shuffle.

    rdd.partitioner match {
      case Some(_) => println("there is default partitioner")
      case _ => println("no default partitioner")
    }
  }
}
