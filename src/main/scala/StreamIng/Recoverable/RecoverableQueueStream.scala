import java.io.File
import StreamIng.Util.GenerateQueueRDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Haha , queue stream doesn't support checkpoint.
 */
object RecoverableQueueStream {

  def createContext(outputPath: String, checkpointDirectory: String)
  : StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context &&&&&&&&&&&&&&&&&&&&&&")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) outputFile.delete()
    val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount")
      .setMaster("local[4]")
    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDirectory)

    val data = ssc.queueStream(GenerateQueueRDD.generateQueueRDD(ssc))

    data.foreachRDD(rdd => rdd.foreach(println))


    ssc
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
