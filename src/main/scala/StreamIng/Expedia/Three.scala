package StreamIng.Expedia

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Three {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("LpasTrends")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // lpasSyntheticShops  uis
    val topicName = "lpasSyntheticShops"
    val topicMap = Map(topicName -> 4)
    val group = "LpasTrends"

    //val zkQuorum = "zk1.us-east-1.prod.expedia.com:2181,zk2.us-east-1.prod.expedia.com:2181,zk3.us-east-1.prod.expedia.com:2181"
    val zkQuorum = "zk-ewep-1.us-west-2.test.expedia.com:2181/kafka/ewep," +
      "zk-ewep-2.us-west-2.test.expedia.com:2181/kafka/ewep," +
      "zk-ewep-3.us-west-2.test.expedia.com:2181/kafka/ewep"

    // create kafka stream and get uis object then filter to get tuple
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
                .map(p => (p._1.toString, p._2.toString))
                .transform(rdd => rdd.reduceByKey((str1, str2) => Math.min(str1.toLong, str2.toLong).toString, 4))

    //(new HashPartitioner(4), p => Math.min(p._1, p._2).toString))
    val updateFunc = (values: Seq[String], state: Option[String]) => {
      val currentCount = values.length.toString

      val previousCount = state.getOrElse(0).toString

      Some(currentCount + previousCount)
    }

    val newUpdateFunc = (seq: Seq[String], value: Option[String]) => {
      updateFunc(seq, value)
    }

    val initRdd = ssc.sparkContext.emptyRDD[(String, String)]

    // we ops data each 20 seconds
    val twentySecondsData = lines.window(Seconds(20), Seconds(20))

    // get lowest price for each hotel.
    val stateDstream = twentySecondsData.updateStateByKey[String](newUpdateFunc)


    //hash according to hotel id and then update low price in state stream
    //val stateDstream = lines.updateStateByKey(newUpdateFunc, lines.compute(null).get.partitioner.get, true, initRdd)

    //val a :RDD[(String)] = (b: RDD[(String)]) => b

    val transformFunc = (stateRDD: RDD[(String, String)], newRDD: RDD[(String, String)]) => {
      stateRDD
    }

    //def trans(stateRDD: RDD[(String, String)], newRDD: RDD[(String, String)]): RDD[(String, String)] = {
    //  stateRDD
    //}

    // based on low price in state stream, to update new incoming stream and then get the stream can write to redis
    val transformedDstream = stateDstream.transformWith(lines, transformFunc)

    // trigger job and then write to redis
    transformedDstream.foreachRDD(rdd => {
      //val redis = new redisClient()
    })

    lines.foreachRDD(rdd => {
      rdd.foreach(println)
      println("***********")
    })
    println(lines.count())
    println("***********")

    ssc.start()
    ssc.awaitTermination()
  }
}
