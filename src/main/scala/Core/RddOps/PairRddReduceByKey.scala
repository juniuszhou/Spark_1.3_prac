package main.scala.Core.RddOps

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

object PairRddReduceByKey {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "Simple App")

    def GeneratePairRDDWithPartitioner(sc: SparkContext, par: Int): RDD[(Int, String)] = {
      val r: Random = new Random()
      val v: Array[(Int, String)] = new Array[(Int, String)](10)
      (0 to 9).foreach(i => v(i) = (i, r.nextString(r.nextInt(10))))
      sc.parallelize(v, par).reduceByKey(new HashPartitioner(2),
        (str: String, str2: String) => str) // just keep the first element.
    }

    val rdd = GeneratePairRDDWithPartitioner(sc, 2)

    // no shuffle whether the parent rdd has partitioner or not.
    val resRdd = rdd.reduceByKeyLocally((str, str2) => str)
  }
}
