package StreamIng.Recoverable

import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * socket stream can be recovered from driver failure.
 */
object RecoverableSocketStream {
  def printOutputRDD(count: DStream[(String, Int)]){
    count.foreachRDD(rdd =>  { rdd.foreach(u => println( Integer.getInteger(u._1) + " " + u._2))
      println(" rdd split _________________")

    })
  }
  def createContext(outputPath: String, checkpointDirectory: String)
  : StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context &&&&&&&&&&&&&&&&&&&&&&")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) outputFile.delete()
    val sparkConf = new SparkConf().setAppName("RecoverableSocketStream")
      .setMaster("local[4]")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    // Create the context with a 1 second batch size
    val sc = new StreamingContext(sparkConf, Seconds(10))
    val port : Int = 9999
    // socketTextStream just connect to this port and then get data.
    //So you must must must run nc -lk 9999 before run this program.
    val lines = sc.socketTextStream("localhost", port, StorageLevel.MEMORY_ONLY_SER).cache

    val words = lines.flatMap(_.split(" "))
    val counts  = words.map(x => (x, 1)).reduceByKey(_ + _)

    printOutputRDD(counts)
    sc.checkpoint(checkpointDirectory)

    sc
  }


  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("count ")
      .setMaster("local[4]")

    val outputPath = "/home/junius/sparkRunTime/streaming"
    val checkpointDirectory = "/home/junius/sparkRunTime/checkPoint"

    val sc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(outputPath, checkpointDirectory)
      })
    //sc.queueStream()
    //SetLogLevel.setLogLevel()

    sc.start()
    sc.awaitTermination()
  }
}
