package MyGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl

import scala.collection.mutable.Set

/**
 * Created by junius on 15-1-7.
 */
object SecondNeighbor {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[1]", "Simple App")
    val g = GraphGenerator.GenerateCycle(10, sc)
    //GraphPrint.PrintGraph(graphOne)

    //val vNeigh: VertexRDD[Array[VertexId]] = graphOne.collectNeighborIds(EdgeDirection.Either)
    //val sg = GraphImpl(vNeigh, graphOne.edges)
    //GraphPrint.PrintGraph3(sg)

    val nbrSets: VertexRDD[Set[VertexId]] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = Set[VertexId]()
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if(nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }

    val sg = GraphImpl(nbrSets, g.edges)
    GraphPrint.PrintGraphSet(sg)

    def edgeFunc(ctx: EdgeContext[Set[VertexId], Int, Set[VertexId]]): Unit ={
      var msg = ctx.srcAttr
      msg = msg.union(ctx.dstAttr)
      ctx.sendToSrc(msg)
      ctx.sendToDst(msg)
    }

    val n2Neigh =
      sg.aggregateMessages(edgeFunc, (a: Set[VertexId], b: Set[VertexId]) => a.union(b))
    n2Neigh.foreach(x => {
      print(x._1)
      print(" ")
      x._2.foreach(i => print(" " + i))
      println()
    })
  }
}
