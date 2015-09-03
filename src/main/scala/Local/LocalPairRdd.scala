package Local

import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions

/**
 * Created by junius on 14-8-18.
 */


object LocalPairRdd {
  def main(args: Array[String]) {
    val path = "/home/junius/git_hub/spark_project/resource/"
    val numFile = path + "number.txt"
    val wordFile = path + "news.txt"

    val sc = new SparkContext("local[4]", "Simple App")
    val wordData = sc.textFile(wordFile, 3).cache
    val numData = sc.textFile(numFile, 3).cache

    //no such method in 1.0.0
    //val kvData = numData.map(str => (str.toInt, str)).aggregateByKey(0, 3)(_ + _, _ + _)

    //combine values for the same key
   // val kvData: PairRDDFunctions[Int, String] = numData.map(str => (str.toInt, str))
    val otherKvData = numData.map(str => (str.toInt, " " + str + " junius"))
   // kvData.cogroup(otherKvData)

  }
}