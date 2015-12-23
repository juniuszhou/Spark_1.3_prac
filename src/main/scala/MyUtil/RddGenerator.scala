package MyUtil

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by junius on 15-5-24.
 */
object RddGenerator {
  def GenerateFromCollection(sc: SparkContext) : RDD[String] = {
    val r: Random = new Random()
    val v: Array[String] = new Array[String](100)
    (0 to 99).map(i => v(i) = r.nextString(10))
    sc.parallelize(v, 4)
  }

  def GenerateTableString(sc: SparkContext) : RDD[Tuple1[String]] = {
    val r: Random = new Random()
    val v: Array[Tuple1[String]] = new Array[Tuple1[String]](100)
    (0 to 99).foreach(i => v(i) = {
      val one = r.nextString(10)
      val two = r.nextString(10)
      Tuple1(one)
    })
    sc.parallelize(v, 4)
  }

  def GenerateTableString2(sc: SparkContext) : RDD[Product] = {
    val r: Random = new Random()
    val v: Array[Product] = new Array[Product](100)
    (0 to 99).foreach(i => {
      val one = r.nextString(5)
      val two = r.nextString(5)
      v(i) = (one, two)
    })

    sc.parallelize(v, 4)
  }

  def GenerateNumberRDD(sc: SparkContext) : RDD[Int] = {
    val r: Random = new Random()
    val v: Array[Int] = new Array[Int](100)
    (0 to 99).foreach(i => v(i) = r.nextInt)
    sc.parallelize(v, 4)
  }

  def GeneratePairRDD(sc: SparkContext) : RDD[(Int, String)] = {
    val r: Random = new Random()
    val v: Array[(Int,String)] = new Array[(Int,String)](100)
    (0 to 99).foreach(i => v(i) = (r.nextInt, r.nextString(10)))
    sc.parallelize(v, 4)
  }

  def GenerateStrStrRDD(sc: SparkContext) : RDD[(String, String)] = {
    val r: Random = new Random()
    val v: Array[(String,String)] = new Array[(String,String)](100)
    (0 to 99).foreach(i => v(i) = ((r.nextInt & 0xFF).toString, (r.nextInt & 0xFF).toString))
    sc.parallelize(v, 4)
  }

  def GenerateFromFile(sc: SparkContext) : RDD[String] = {
    val logFile = "Input File Path"
    val logData = sc.textFile(logFile, 4)
    val wordData: RDD[String] = logData.flatMap(i => i.split(" "))
    val letterData = wordData.flatMap(line => line.toList)
    letterData.map(println).count()
    wordData
  }


}
