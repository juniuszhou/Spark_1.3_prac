package main.scala.Core

import MyUtil.SetLogLevel
import org.apache.spark.util.Utils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by junius on 15-5-31.
 */
object PrintAllParameters {
  def main(args: Array[String]) {
    SetLogLevel.setLogLevels()

    val sc = new SparkContext("local", "Simple App")

    val sparkConf = sc.getConf

    sparkConf.getAll.foreach(strs => println("name: " + strs._1 + "   value: " + strs._2))

    println(System.getProperty("user.dir"))


    sc.stop()

  }
}
