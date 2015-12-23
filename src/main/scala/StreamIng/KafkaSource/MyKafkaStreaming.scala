package StreamIng.KafkaSource

/*
import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties

import kafka.serializer.StringDecoder
import org.apache.commons.io.Charsets
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.concurrent.ExecutionContext.Implicits.global
*/

object MyKafkaStreaming {
}

  /*
object Constants {
  val NumPublishers = 5
  val NumAdvertisers = 3

  val Publishers = (0 to NumPublishers).map("publisher_" +)
  val Advertisers = (0 to NumAdvertisers).map("advertiser_" +)
  val UnknownGeo = "unknown"
  val Geos = Seq("NY", "CA", "FL", "MI", "HI", UnknownGeo)
  val NumWebsites = 10000
  val NumCookies = 10000

  val KafkaTopic = "adnetwork-topic"
}

case class ImpressionLog(timestamp: Long, publisher: String, advertiser: String,
                         website: String, geo: String, bid: Double, cookie: String)

// intermediate result used in reducer
case class AggregationLog(timestamp: Long, sumBids: Double, imps: Int = 1)

// result to be stored in MongoDB
case class AggregationResult(date: Long, publisher: String, geo: String, imps: Int, uniques: Int, avgBids: Double)

case class PublisherGeoKey(publisher: String, geo: String)

/*
class ImpressionLogDecoder(props: VerifiableProperties) extends Decoder[ImpressionLog] {
  def fromBytes(bytes: Array[Byte]): ImpressionLog = {
    salat.grater[ImpressionLog].fromJSON(new String(bytes, Charsets.UTF_8))
  }
}

class ImpressionLogEncoder(props: VerifiableProperties) extends Encoder[ImpressionLog] {
  def toBytes(impressionLog: ImpressionLog): Array[Byte] = {
    salat.grater[ImpressionLog].toCompactJSON(impressionLog).getBytes(Charsets.UTF_8)
  }
}
*/

  def main(args: Array[String]) {
    val BatchDuration = Seconds(10)

    val sparkContext = new SparkContext("local[4]", "logAggregator")

    // we discretize the stream in BatchDuration seconds intervals
    val streamingContext = new StreamingContext(sparkContext, BatchDuration)

    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "zookeeper.connection.timeout.ms" -> "10000",
      "group.id" -> "myGroup"
    )

    val topics = Map(
      Constants.KafkaTopic -> 1
    )

    // stream of (topic, ImpressionLog)
    val messages = KafkaUtils.createStream[String, ImpressionLog, StringDecoder, ImpressionLogDecoder](streamingContext, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)

    // to count uniques
    //lazy val hyperLogLog = new HyperLogLogMonoid(12)

    // we filter out non resolved geo (unknown) and map (pub, geo) -> AggLog that will be reduced
    val logsByPubGeo = messages.map(_._2).filter(_.geo != Constants.UnknownGeo).map {
      log =>
        val key = PublisherGeoKey(log.publisher, log.geo)
        val agg = AggregationLog(
          timestamp = log.timestamp,
          sumBids = log.bid,
          imps = 1,
          uniquesHll = hyperLogLog(log.cookie.getBytes(Charsets.UTF_8))
        )
        (key, agg)
    }

    // Reduce to generate imps, uniques, sumBid per pub and geo per interval of BatchDuration seconds
    import org.apache.spark.streaming.StreamingContext._
    val aggLogs = logsByPubGeo.reduceByKeyAndWindow(reduceAggregationLogs, BatchDuration)

    // Store in MongoDB
    aggLogs.foreachRDD(saveLogs(_))

    // start rolling!
    streamingContext.start()

     def saveLogs(logRdd: RDD[(PublisherGeoKey, AggregationLog)])
    {

    }

     def reduceAggregationLogs(aggLog1: AggregationLog, aggLog2: AggregationLog) =
    {

    }
  }
}

    */