package main.scala.Core.RddOps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by junius on 15-1-15.
 */
object RddGenerator2 {
  def GenerateFromCollection(sc: SparkContext) : RDD[String] = {
    val r: Random = new Random()
    val v: Array[String] = new Array[String](100)
    (0 to 99).map(i => v(i) = r.nextString(10))
    sc.parallelize(v, 4)
  }

  def GenerateNumberRDD(sc: SparkContext) : RDD[Int] = {
    val r: Random = new Random()
    val v: Array[Int] = new Array[Int](100)
    (0 to 99).map(i => v(i) = r.nextInt)
    sc.parallelize(v, 4)
  }

  def GeneratePairRDD(sc: SparkContext) : RDD[(Int, String)] = {
    val r: Random = new Random()
    val v: Array[(Int,String)] = new Array[(Int,String)](100)
    (0 to 99).map(i => v(i) = (r.nextInt, r.nextString(10)))
    sc.parallelize(v, 4)
  }

  def GenerateStrStrRDD(sc: SparkContext) : RDD[(String, String)] = {
    val r: Random = new Random()
    val v: Array[(String,String)] = new Array[(String,String)](100)
    (0 to 99).map(i => v(i) = ((r.nextInt & 0xFF).toString, (r.nextInt & 0xFF).toString))
    sc.parallelize(v, 4)
  }

  def GenerateFromFile(sc: SparkContext) : RDD[String] = {
    val logFile = "Input File Path"
    val logData = sc.textFile(logFile, 4)
    val wordData: RDD[String] = logData.flatMap(i => i.split(" "))
    val letterData = wordData.flatMap(line => line.toList)
    letterData.map(println).count
    wordData
  }


}
