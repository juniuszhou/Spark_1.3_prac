package Deploy

import org.apache.spark.{SparkContext, SparkConf}

object DeployYarnClient{
  def main (args: Array[String]) {

    val conf = new SparkConf()
      //.setMaster("yarn://54.222.137.50:8030")
      .setAppName("Simple app")

    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4),2)
    rdd.foreach(println(_))
    sc.stop()
  }
}