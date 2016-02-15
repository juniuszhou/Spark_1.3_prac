package StreamIng.Expedia

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json._

object DirectKafkaStreamCassandra {
  def main (args: Array[String]) {
    SetLogLevel.setLogLevels()

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LpasPricingAnalysis")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.cassandra.connection.host", "10.2.12.115")

    /** Creates the keyspace and table in Cassandra.
    CassandraConnector(sparkConf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS lpas WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS lpas.LpasPricingAnalysis (hotelId TEXT PRIMARY KEY, available TEXT)")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Create direct kafka stream with brokers and topics
    val brokers = "10.2.91.163:9092,10.2.89.157:9092,10.2.90.1:9092,10.2.89.85:9092"
    val topicsSet = Set("lpasPricingAnalysis")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val data = messages.map(_._2)

    //parse and put data to cassandra.
    data.flatMap(str => {
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
                  l.toList.map(map => (
                    map.get("hotelId").get.toString.toFloat.toLong.toString,
                    map.get("available").get.toString
                    ))
                }
                case None => Nil
              }
            }
            case scala.util.Failure(ss) => Nil
          }
        }
        case None => Nil
      }
    }).saveToCassandra("lpas", "lpaspricinganalysis")

    ssc.start()
    ssc.awaitTermination()

      */
  }
}
