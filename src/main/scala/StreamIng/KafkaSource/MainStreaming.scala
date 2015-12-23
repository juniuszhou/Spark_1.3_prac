package StreamIng.KafkaSource

import org.apache.spark._

object MainStreaming {
  def main (args: Array[String]) {
    //.setMaster("local[4]")

    val conf = new SparkConf()
      .setMaster("spark://10.2.70.174:7077")
      .setAppName("junius")
    .set("spark.driver.allowMultipleContexts", "true")

      //.set("SPARK_WORKER_CORES","1")
      //.set("SPARK_WORKER_MEMORY","1g")

    val sc = new SparkContext(conf)
    //val ssc = new StreamingContext(conf, Seconds(1))
    println("start")
    val ll = List(0,1)
    val data = sc.parallelize(ll, 2)
    println(data.count())
    println("end")
    Thread.sleep(10000)

    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
