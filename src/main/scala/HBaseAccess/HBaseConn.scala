package HBaseAccess

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext

/**
 * https://github.com/apache/hbase
 * https://github.com/apache/hbase/blob/master/hbase-spark/src/main/scala/org/apache/hadoop/hbase/spark/HBaseContext.scala
 *
 *
 */
object HBaseConn {
  def main (args: Array[String]) {
    val config = HBaseConfiguration.create()

    val sc = new SparkContext("local[4]", "test")

    val list = List(0,1,2)

    // val hbaseContext = new HBaseContext(sc, config)


  }
}
