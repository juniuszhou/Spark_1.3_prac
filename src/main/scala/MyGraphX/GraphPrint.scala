package MyGraphX

import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl

/**
 * Created by junius on 15-1-7.
 */
object GraphPrint {
  def PrintGraph(g : Graph[_, _]) = {

    g.vertices.mapPartitionsWithIndex(
      (pid, vids) => {
        vids.map(x => println("Vertex pid is " + pid + " vertex id is " + x._1 + " value is  " + x._2))
      }).count()

    g.edges.mapPartitionsWithIndex((pid, edges) =>
      edges.map(e =>
        println("Edge pid is " + pid + " src " + e.srcId + " dst " + e.dstId + " attr " + e.attr))).count
  }


  def PrintGraph3(g: Graph[Array[VertexId], Int]): Unit ={

    g.vertices.mapPartitionsWithIndex(
      (pid, vids) => {
        vids.map(x => {
          print("Vertex pid is " + pid)

          print(" vertex id is " + x._1)
          x._2.foreach(i => print(" JJKK " + i))
          println()
        })
      }).count()

    g.edges.mapPartitionsWithIndex((pid, edges) =>
      edges.map(e =>
        println("Edge pid is " + pid + " src " + e.srcId + " dst " + e.dstId + " attr " + e.attr))).count
  }

  def PrintGraphSet(g: Graph[scala.collection.mutable.Set[VertexId], Int]): Unit ={

    g.vertices.mapPartitionsWithIndex(
      (pid, vids) => {
        vids.map(x => {
          print("Vertex pid is " + pid)

          print(" vertex id is " + x._1)
          x._2.foreach(i => print(" JJKK " + i))
          println()
        })
      }).count()

    g.edges.mapPartitionsWithIndex((pid, edges) =>
      edges.map(e =>
        println("Edge pid is " + pid + " src " + e.srcId + " dst " + e.dstId + " attr " + e.attr))).count
  }

}
