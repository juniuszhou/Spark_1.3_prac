package MyGraphX
import MyGraphX.GraphGenerator

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

/**a
 * Created by junius on 14-12-26.
 */
object EdgeRddOps {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App")
    var edges: Set[Edge[Int]] = Set()
    (0 to 3).map(i => {
      (0 to 3).map(j => edges += Edge[Int](i, j, 1))
    })

    // parallelize the edges and generate RDD. of course, graph partition according to edge.
    val rdd: RDD[Edge[Int]] = sc.parallelize(edges.toList)
    rdd.map(_.copy())

    rdd.collect().foreach(println)

    // collect.
    //rdd.dependencies.head.rdd.asInstanceOf[RDD[Edge[Int]]].iterator(1, sc)

  }
}
