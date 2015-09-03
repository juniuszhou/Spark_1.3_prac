package MyGraphX

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * Created by junius on 15-1-3.
 */
object GraphAPI {






  def myMapVertices[VD: ClassTag](g : Graph[Int, Int]): Unit = {
    val f = (vid: VertexId, value: Int) => vid + value
    GraphPrint.PrintGraph(g.mapVertices(f))
  }

  def myMapEdges[VD: ClassTag](g : Graph[Int, Int]): Unit = {
    /*  what's wrong still unknown.
    val f = (pid: PartitionID, value: Iterator[Edge[Int]]) => {

      val l : List[Int] = Nil
      value.foreach(e => (e.srcId + e.dstId + e.attr) :: l)
      l.iterator.toIterator
    }
    */


    val f = (pid: PartitionID, values: Iterator[Edge[Int]]) => {
      val m = (e: Edge[Int]) => pid + e.srcId + e.dstId + e.attr
      values.map(m)
    }
    GraphPrint.PrintGraph(g.mapEdges(f))
  }

  def myMapEdges2[VD: ClassTag](g : Graph[Int, Int]): Unit = {
    val f = (e: Edge[Int]) => e.srcId + e.dstId + e.attr
    GraphPrint.PrintGraph(g.mapEdges(f))
  }

  def myMapTriplets[VD: ClassTag](g : Graph[Int, Int]): Unit = {
    // the result apply to edge 's attribute.
    val f = (et: EdgeTriplet[Int, Int]) => et.srcId + et.dstId + et.attr + et.srcAttr + et.dstAttr
    GraphPrint.PrintGraph(g.mapTriplets(f))
  }

  def myMapTriplets2[VD: ClassTag](g : Graph[Int, Int]): Unit = {
    // the result apply to edge 's attribute.
    val f = (pid: PartitionID, values: Iterator[EdgeTriplet[Int, Int]]) => {
      val m = (et: EdgeTriplet[Int, Int]) => pid + et.srcId + et.dstId + et.attr + et.srcAttr + et.dstAttr
      values.map(m)
    }
    GraphPrint.PrintGraph(g.mapTriplets(f, TripletFields.All))
  }

  def getAllNeighborIds[VD: ClassTag](g : Graph[Int, Int]): Unit = {
    val newV : VertexRDD[Array[VertexId]] = g.collectNeighborIds(EdgeDirection.In)
    val newG = Graph(newV, g.edges)
    GraphPrint.PrintGraph3(newG)
  }

  def MyGroupEdges(g: Graph[Int, Int]) = {
    val f = (a: Int, b: Int) => a + b
    GraphPrint.PrintGraph(g.groupEdges(f))
  }

  def MyOuterJoinVertices(g: Graph[Int, Int], sc: SparkContext, i: Int) = {
    var vertices: Set[(VertexId, Int)] = Set()
    (0 to i).foreach(x => vertices += Tuple2(x.toLong, x * 100))
    val rdd = sc.parallelize(vertices.toList, 4)
    val f = (id: VertexId, ge: Int, bai: Option[Int]) => ge + id + bai.getOrElse(100)
    GraphPrint.PrintGraph(g.outerJoinVertices(rdd)(f))
  }

  def main (args: Array[String]) {
    val sc = new SparkContext("local[1]", "Simple App")
    val graphOne = GraphGenerator.GenerateAllLinkedGraph(7, sc)
    GraphPrint.PrintGraph(graphOne)

    //myMapVertices(graphOne)

    //myMapEdges(graphOne)

    //myMapTriplets2(graphOne)

    // getAllNeighborIds(graphOne)

    // MyGroupEdges(graphOne)

    MyOuterJoinVertices(graphOne, sc , 7)
  }
}
