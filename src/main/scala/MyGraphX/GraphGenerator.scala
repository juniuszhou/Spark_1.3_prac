package MyGraphX

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

// junius, to support implicit transform from RDD to pairRDD, below must be included.
import org.apache.spark.SparkContext._
/**
 * Created by junius on 14-12-25.
 */
object GraphGenerator {
  def GenerateNSplitGraph(numVertices: Int, sc: SparkContext, group: Int): Graph[Int, Int] = {
    var edges: Set[Edge[Int]] = Set()
    (0 to group).map(i => {
      edges += Edge[Int](i * 10, i * 10 + numVertices, 1)
      (0 to numVertices).map(j => {
         edges += Edge[Int](i * 10 + j , i * 10 + j + 1, 1)
      })
    })
    val rdd: RDD[Edge[Int]] = sc.parallelize(edges.toList, 4)

    Graph.fromEdges(rdd, 0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
  }

  def GenerateAllLinkedGraph(numVertices: Int, sc: SparkContext): Graph[Int, Int] ={
    var edges: Set[Edge[Int]] = Set()
    (0 to numVertices).map(i => {
      (0 to numVertices).map(j => edges += Edge[Int](i, j, 1))
    })

    // parallelize the edges and generate RDD. of course, graph partition according to edge.
    val rdd: RDD[Edge[Int]] = sc.parallelize(edges.toList, 4)

    Graph.fromEdges(rdd, 0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
  }

  def GenerateAllLinkedGraph2(numVertices: Int, sc: SparkContext): Graph[Int, Int] ={
    var edges: Set[Edge[Int]] = Set()
    (0 to numVertices).map(i => {
      (0 to numVertices).map(j => edges += Edge[Int](i, j, 1))
    })

    // parallelize the edges and generate RDD. of course, graph partition according to edge.
    val rdd: RDD[Edge[Int]] = sc.parallelize(edges.toList)
    val vertices: RDD[(VertexId, Int)] = rdd.flatMap(edge => List((edge.srcId, 1)))
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => (vid, degree) }

    // based on vertices and their attributes
    //          edges and their attributes
    // we can call Graph to create a graph. actual graphImp will be generated implicitly.
    Graph(vertices, rdd, 0)
  }


  def GenerateCycle(numVertices: Int, sc: SparkContext): Graph[Int, Int] = {
    var edges: Set[Edge[Int]] = Set()
    (0 to numVertices - 1).map(i => edges += Edge[Int](i, i+1, 1))
    edges += Edge[Int](numVertices, 0 , 1)
    val rdd: RDD[Edge[Int]] = sc.parallelize(edges.toList, 4)
    Graph.fromEdges(rdd, 0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
  }
}
