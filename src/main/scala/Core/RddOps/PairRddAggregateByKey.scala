package main.scala.Core.RddOps

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

object PairRddAggregateByKey {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[2]", "Simple App")

    def GeneratePairRDDWithPartitioner(sc: SparkContext, par: Int): RDD[(Int, String)] = {
      val r: Random = new Random()
      val v: Array[(Int, String)] = new Array[(Int, String)](10)
      (0 to 9).foreach(i => v(i) = (i, r.nextString(r.nextInt(10))))
      sc.parallelize(v, par).reduceByKey(new HashPartitioner(2),
        (str: String, str2: String) => str)
    }

    val rdd = GeneratePairRDDWithPartitioner(sc, 2)

    // if rdd has partitioner, then no shuffle for this op.
    // otherwise, there is a shuffle process.
    val resRdd = rdd.aggregateByKey(0)(
      (len: Int, str2: String) => len + str2.length,
      //(len1: Int, len2:Int) => len1 + len2)
      _ + _)
    println(resRdd.toDebugString)
  }
}
