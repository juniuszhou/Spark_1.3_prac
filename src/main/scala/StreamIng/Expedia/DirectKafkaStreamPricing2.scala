package StreamIng.Expedia

//import com.mongodb.casbah.commons.MongoDBObject
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json._

object DirectKafkaStreamPricing2 {
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
      println("@@@@@ " + str)
      val json = JSON.parseFull(str)
      json match {
        case Some(m) => {
          val map = m.asInstanceOf[Map[String,String]]
          map.foreach(str => println("&& " + str._1 + " " + str._2))
          val base = map.get("message").get
          val decode = Base64Decoder.decodeUnzip(base)

          decode match {
            case scala.util.Success(ss) => {
              println("$#$# " + ss)
              val hotels = JSON.parseFull(ss)
              hotels match {
                case Some(h) => {
                  val map = h.asInstanceOf[Map[String, List[Map[String, Any]]]]
                  map.foreach(str => println("(( " + str._1))

                  val l = map.get("hotelDetails").get
                  l.foreach(m => {
                    val hotelId = m.get("hotelId").get.toString.toFloat.toLong.toString
                    println("~~~~~~ hotelId " + m.get("hotelId").get.toString)
                    println("~~~~~~ available " + m.get("available").get.toString)
                    //val old = MongoDBObject("hotelId" -> hotelId)
                    //val updated = MongoDBObject("hotelId" -> hotelId,
                    //  "available" -> (m.get("available").get.toString + " dsfdsfads"))
                    //coll.update(old, updated, true)
                  })
                }
                case None => "nothing in hotels."
              }
            }
            case scala.util.Failure(ss) => println("$#$# exception " + ss)
          }
        }
        case None => println("nothing")
      }

    }))

    ssc.start()
    ssc.awaitTermination()
  }
}
