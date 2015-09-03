package Deploy

import java.util.Random

import org.apache.spark.SparkContext

object CoreWordCount{
  def main(args: Array[String]) {
    val sc = new SparkContext("spark://172.17.0.2:7077", "Simple App")

    val random = new Random()
    val dataLen = 1000
    //generate so many integer
    val dataSet: Array[Int] = new Array[Int](dataLen)
    (0 until dataLen).map(index => (dataSet(index) = random.nextInt()))

    val dataRdd = sc.makeRDD(dataSet, 10).cache

    //get rdd from local file
    // val logFile = "/home/junius/my_git/spark_project/README.md"
    // Should be some file on your system

    //get rdd from hdfs.
    // val logFile = "hdfs://172.16.32.1:9000/input/number.txt"

    //val sc = new SparkContext("local[4]", "Simple App")
    //ip as virtual one assigned from vm station.


    //if use ubuntu, doesn't work. reason is unclear.
    //val sc = new SparkContext("spark://ubuntu:7077", "Simple App")

    //cache rdd to memory
    val logData = dataRdd.count()
    print("junius get")
  }
}


