package main.scala.Core.RddOps

import main.scala.RddGenerator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by junius on 15-8-7.
 */
object MyJoin {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[2]", "Simple App")

    def GeneratePairRDD(sc: SparkContext, par: Int) : RDD[(Int, String)] = {
      val r: Random = new Random()
      val v: Array[(Int,String)] = new Array[(Int,String)](1000000)
      (0 to 999999).foreach(i => v(i) = (r.nextInt, "a"+par))
      sc.parallelize(v, par)
    }

    val rdd =  GeneratePairRDD(sc, 1).persist()
    val rdd2 =  GeneratePairRDD(sc, 7).persist()

    val rdd3 = rdd.join(rdd2, 9).persist()
    println(rdd3.toDebugString)
    //rdd3.count()
    rdd3.collect().foreach(iss => println(iss._1.toString + " " + iss._2._1 + " " + iss._2._2))
    //rdd3.foreach(println)

  }
}
