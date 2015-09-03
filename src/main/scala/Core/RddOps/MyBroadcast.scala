package main.scala.Core.RddOps

import org.apache.spark.SparkContext

/**
 * Created by junius on 15-1-15.
 */
object MyBroadcast {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App")
    // myCombineByKey(sc)
    val bc = sc.broadcast[String]("junius")
    // bc.

    sc.stop
  }
}
