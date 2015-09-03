package MyGraphX

import java.util

import org.apache.spark.graphx.{VertexId, EdgeTriplet, Edge}

class MyEdgeTrip[VD, ED](val src: VertexId = 0,
                         val dst: VertexId = 0,
                         val att: ED = null.asInstanceOf[ED])
  extends Edge[ED](src, dst, att){
  var srcAttr: VD = _ // nullValue[VD]
  var dstAttr: VD = _ // nullValue[VD]
}

object GraphItemOps {
  def main (args: Array[String]) {
    val anEdge = new Edge(1, 1, "edge one")
    println(anEdge.toString)

    val edgeMultiAttr = new Edge(1, 2, new util.ArrayList[String]())
    println(edgeMultiAttr.toString)

    val edgeTrip = new EdgeTriplet[String, String]()
    edgeTrip.srcAttr = "source"
    edgeTrip.dstAttr = "destination"
    edgeTrip.srcId = 1
    edgeTrip.dstId = 2
    edgeTrip.attr = "edge"
    println(edgeTrip.toString)
    println(edgeTrip.toTuple)


    val met = new MyEdgeTrip[String, String](1,2,"edge one")
    met.srcAttr = "source"
    met.dstAttr = "dest"
    println(met.toString)

  }
}
