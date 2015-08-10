package main.scala.Core.RddOps

import main.scala.RddGenerator
import org.apache.spark.SparkContext

/**
 * Created by junius on 15-8-9.
 */
object MyGroupByKey {
  def main (args: Array[String]) {
    val sc = new SparkContext("local", "Simple App")
    val rdd =  RddGenerator.GeneratePairRDD(sc)

    val rdd3 = rdd.groupByKey()
    println(rdd3.toDebugString)
  }
}
