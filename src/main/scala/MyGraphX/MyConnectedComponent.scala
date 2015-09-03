package MyGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeTriplet, VertexId, Pregel}

/**
 * Created by junius on 15-1-13.
 */
object MyConnectedComponent {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[1]", "Simple App")
    val g = GraphGenerator.GenerateNSplitGraph(4, sc, 4)
    GraphPrint.PrintGraph(g)
    val g1 = g.mapVertices((id, value) => scala.collection.mutable.Set(id)).cache()
    // call pregel
    val prog = (id: VertexId, s1: scala.collection.mutable.Set[VertexId], s2: scala.collection.mutable.Set[VertexId]) => {
      s1 ++ s2
    }
    def send(edge: EdgeTriplet[scala.collection.mutable.Set[VertexId], Int]) = {
      val s1 = edge.srcAttr
      val s2 = edge.dstAttr
      if (s1 == s2) {
        Iterator.empty
      } else {
        val s3 = s1 ++ s2
        Iterator((edge.srcId, s3), (edge.dstId, s3))
      }
    }
    def merge(s1: scala.collection.mutable.Set[VertexId], s2: scala.collection.mutable.Set[VertexId]) = s1 ++ s2
    val init = scala.collection.mutable.Set[VertexId]()
    GraphPrint.PrintGraphSet(Pregel(g1, init)(vprog=prog, sendMsg=send, mergeMsg=merge))


  }
}
