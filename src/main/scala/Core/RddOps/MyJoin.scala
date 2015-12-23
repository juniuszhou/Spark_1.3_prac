package main.scala.Core.RddOps

import MyUtil.RddGenerator
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
      val v: Array[(Int,String)] = new Array[(Int,String)](10)
      (0 to 9).foreach(i => v(i) = (i, "a"+par))
      sc.parallelize(v, par)
    }

    val rdd =  GeneratePairRDD(sc, 2).persist()
    println(rdd.partitioner)
    val rdd2 =  GeneratePairRDD(sc, 2).persist()

    val rdd3 = rdd.join(rdd2, 2).persist()
    println("--------------------------")

    println("--------------------------")
    println(rdd3.toDebugString)
    rdd3.count()
    println("--------------------------")
    //rdd3.collect().foreach(iss => println(iss._1.toString + " " + iss._2._1 + " " + iss._2._2))
    //rdd3.foreach(println)
    Thread.sleep(1000 * 1000)

  }
}
