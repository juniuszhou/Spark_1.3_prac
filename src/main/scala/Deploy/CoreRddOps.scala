/**
 * Created by junius on 14-8-18.
 */

// package name for deployed via this package.
package Deploy

import java.net.InetAddress

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer


//this one successfully run at spark cluster.
object CoreRddOps {


  def OperateOnPreferredLocation(sc: SparkContext): Unit ={
    val host1Data = "ubuntu data "
    val host2Data = "ubuntu2 data "
    val host3Data = "ubuntu3 data "

    val big1 = (0 to 10000).map(i => (host1Data + i))
    val big2 = (0 to 10000).map(i => (host2Data + i))
    val big3 = (0 to 10000).map(i => (host3Data + i))

    //different location priority for different data
    val l1 = List("ubuntu", "ubuntu2", "ubuntu3")
    val l2 = List("ubuntu2", "ubuntu", "ubuntu3")
    val l3 = List("ubuntu3", "ubuntu2", "ubuntu")

    /*
    val buf = new ListBuffer[(String, Seq[String])]()
    buf.append((host1Data, l1))
    buf.append((host2Data, l2))
    buf.append((host3Data, l3))

    val data: RDD[Seq[String]] = sc.makeRDD(buf)
    data.cache.map({ str: String =>
      println(str + " " + InetAddress.getLocalHost().getHostName() )
      str } ).count

    */
    val buf = new ListBuffer[(Seq[String], Seq[String])]()
    buf.append((big1, l1))
    buf.append((big2, l2))
    buf.append((big3, l3))


    val data: RDD[Seq[String]] = sc.makeRDD(buf)
    data.cache.map({ str: Seq[String] =>
      println(str + " " + InetAddress.getLocalHost().getHostName() )
      str } ).count

  }

  def main(args: Array[String]) {
    val sc = new SparkContext("spark://172.16.32.1:7077", "CoreRddOps")

    OperateOnPreferredLocation(sc)


  }
}


