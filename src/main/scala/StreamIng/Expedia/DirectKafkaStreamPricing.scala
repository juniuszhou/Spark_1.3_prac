package StreamIng.Expedia

//import com.mongodb.casbah.commons.MongoDBObject
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json._

object DirectKafkaStreamPricing {
  def main (args: Array[String]) {
    SetLogLevel.setLogLevels()

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LpasPricingAnalysis")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Create direct kafka stream with brokers and topics
    val brokers = "10.2.91.163:9092,10.2.89.157:9092,10.2.90.1:9092,10.2.89.85:9092"
    val topicsSet = Set("lpasPricingAnalysis")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val data = messages.map(_._2)
    lazy val coll = MongoAccess.getMongoCollection("price")

    data.foreachRDD(rdd => rdd.foreach(str => {
      JSON.parseFull(str) match {
        case Some(m) => {
          val map = m.asInstanceOf[Map[String,String]]
          val base = map.get("message").get
          Base64Decoder.decodeUnzip(base) match {
            case scala.util.Success(ss) => {
              JSON.parseFull(ss) match {
                case Some(h) => {
                  val map = h.asInstanceOf[Map[String, List[Map[String, Any]]]]
                  val l = map.get("hotelDetails").get
                  l.foreach(m => {
                    val hotelId = m.get("hotelId").get.toString.toFloat.toLong.toString
                    //val old = MongoDBObject("hotelId" -> hotelId)
                   // val updated = MongoDBObject("hotelId" -> hotelId,
                     // "available" -> m.get("available").get.toString)
                    //coll.update(old, updated, true)
                  })
                }
                case None => "nothing in hotels."
              }
            }
            case scala.util.Failure(ss) => println(ss.toString)
          }
        }
        case None => println("nothing")
      }
    }))

    ssc.start()
    ssc.awaitTermination()
  }
}
