package StreamIng.Expedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

object SetLogLevel extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.ERROR)
    }
  }
}

object Second {
  def main (args: Array[String]) {
    SetLogLevel.setLogLevels()

    val sparkConf = new SparkConf().setMaster("local[2]")setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //ssc.checkpoint("/data/checkpoint")

    // lpasSyntheticShops  uis
    val topicName = "lpasSyntheticShops"
    val topicMap = Map(topicName -> 1)
    val group = "beijing2"
    //val zkQuorum = "zk1.us-east-1.prod.expedia.com:2181,zk2.us-east-1.prod.expedia.com:2181,zk3.us-east-1.prod.expedia.com:2181"
    val zkQuorum = "zk-ewep-1.us-west-2.test.expedia.com:2181/kafka/ewep," +
      "zk-ewep-2.us-west-2.test.expedia.com:2181/kafka/ewep," +
      "zk-ewep-3.us-west-2.test.expedia.com:2181/kafka/ewep"
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    lines.foreachRDD(rdd => {
      rdd.foreach(str => println(str))
      println("***********")
    })
    //println(lines.count())
    //println("***********")

    ssc.start()
    ssc.awaitTermination()
  }
}
