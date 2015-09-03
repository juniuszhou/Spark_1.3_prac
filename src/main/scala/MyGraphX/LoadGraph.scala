package MyGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, GraphLoader}
/**
 * Created by junius on 14-12-22.
 */
object LoadGraph {
  def main (args: Array[String]) {
    /* file format. just src vertex and dst vertex, no attribute for edge.
    2 1
    4 1
    1 2
    6 3
    7 3
    7 6
    6 7
    3 7
     */
    val path = "/home/junius/my_git/spark_1.1.0_comments/graphx/data/followers.txt"
    val sc = new SparkContext("local[4]", "Simple App")
    val g = GraphLoader.edgeListFile(sc, path, numEdgePartitions=2)
    val v = g.vertices
    val e = g.edges

    v.foreach(t => println(t._1.toString + " vertex " + t._2))
    e.foreach(t => println(t.toString + " edge "))
    println(g.toString)

  }

}
