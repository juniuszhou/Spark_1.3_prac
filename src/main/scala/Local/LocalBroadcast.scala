package Local

import org.apache.spark.{SparkContext, SparkConf}

object LocalBroadcast {
  def main (args: Array[String]) {
    val bcName = if (args.length > 2) args(2) else "Http"
    val blockSize = if (args.length > 3) args(3) else "4096"

    System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast." + bcName +
      "BroadcastFactory")
    System.setProperty("spark.broadcast.blockSize", blockSize)
    val sparkConf = new SparkConf().setAppName("Broadcast Test")
                                   .setMaster("local[4]")

    val sc = new SparkContext(sparkConf)

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 100

    val arr1 = new Array[Int](num)
    for (i <- 0 until arr1.length) {
      arr1(i) = i
    }

    //for (i <- 0 until 3) {
      //println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)

      //set barr1 as broadcast data then each worker can get this data.
      // then we can plug the barr1 as source data in RDD.
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.foreach(println(_)))
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      // println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    //}

    sc.stop()
  }
}
