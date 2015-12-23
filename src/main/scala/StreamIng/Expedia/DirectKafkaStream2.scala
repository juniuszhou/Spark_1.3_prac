package StreamIng.Expedia

//import com.redis.RedisClient
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.thrift.TDeserializer
//import org.apache.thrift.protocol.TJSONProtocol

//
object DirectKafkaStream2 {
  def main (args: Array[String]) {
    SetLogLevel.setLogLevels()

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LpasTrends")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Create direct kafka stream with brokers and topics
    val brokers = "10.2.91.163:9092,10.2.89.157:9092,10.2.90.1:9092,10.2.89.85:9092"
    val topicsSet = Set("uis")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val data = messages.map(_._2)
    data.foreachRDD(rdd => rdd.foreach(str => {
      //println("@@@@@ " + str)
      /*
      val deserializer: TDeserializer = new TDeserializer(new TJSONProtocol.Factory)

        val uis: UserInteraction = new UserInteraction()
        deserializer.deserialize(uis, str.getBytes())
      if (uis.logSourceType != null && uis.logSourceType != "ExpWeb") println(uis.logSourceType)

      */
    }))

    ssc.start()
    ssc.awaitTermination()
  }
}
