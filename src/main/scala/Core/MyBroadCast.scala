package main.scala.Core

import MyUtil.RddGenerator
import org.apache.spark.SparkContext

/**
 * Created by junius on 15-6-7.
 */
object MyBroadCast {
  def main (args: Array[String]) {
    val sc = new SparkContext("local", "Simple App")
    val rdd =  RddGenerator.GenerateFromCollection(sc)

    for(i <- List(0,1)) {
      val broad = sc.broadcast(" junius " + i.toString)
      rdd.map(str => str + broad.value).foreach(println)
    }

    sc.stop()
  }

}
