package StreamIng.Expedia

//import com.redis.RedisClient

//class MyRedisClient(override val host: String, override val port: Int) extends RedisClient(host, port) with Serializable{

//}
//
object DirectKafkaStream {
  /*
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
    val topicsSet = Set("lpasSyntheticShops")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val data = messages.map(_._2)
    data.foreachRDD(rdd => rdd.foreach(str => println("@@@@@ " + str)))

    // deal with data each 20 seconds
    val window = data.window(Seconds(20), Seconds(20))

    //parse data from json
    val kvRdd = window.map(str => {
      println("$$$$$$$ " + str)
      val dataMap = JSON.parseFull(str).get.asInstanceOf[Map[String, String]]
      val hotelId = dataMap.get("hotelId").get
      val tpid = dataMap.get("tpid").get
      val checkInDate = dataMap.get("checkin").get
      val price = dataMap.get("price").get
      val roomType = dataMap.get("roomType").get
      val ratePlan = dataMap.get("ratePlan").get
      val key = hotelId + ":" + tpid + ":" + checkInDate
      (key, (price, roomType, ratePlan))
    }).reduceByKey((firstTuple, secondTuple) =>
      if (firstTuple._1.toFloat > firstTuple._1.toFloat) secondTuple else firstTuple)

    //aggregate data and keep lowest price.
    val cacheRdd = kvRdd.updateStateByKey[(String,String,String)]((seq: Seq[(String,String,String)], opt: Option[(String,String,String)]) => {
      if (seq.isEmpty) opt else {
        val min = seq.map(triple => (triple._1.toFloat, (triple._2, triple._3))).toArray.sortBy(_._1)
        opt match {
          case triple: Some[(String,String,String)] =>
            if (min.head._1 > triple.get._1.toFloat) triple
            else Some((min.head._1.toString, min.head._2._1, min.head._2._2))
          case None => Some((min.head._1.toString, min.head._2._1, min.head._2._2))
        }
      }
    })

    //use lowest price to update data in this interval.
    val transformedRdd = kvRdd.transformWith[(String,(String,String,String)), (String,(String,String,String))](cacheRdd,
      (incomingRdd: RDD[(String,(String,String,String))], stateRdd: RDD[(String,(String,String,String))]) => {
        incomingRdd.leftOuterJoin(stateRdd, new HashPartitioner(4))
          .map(kv => {
            kv._2._2 match {
            case triple: Some[(String,String,String)] =>
              if (kv._2._1._1.toFloat > triple.get._1.toFloat) (kv._1, triple.get)
              else (kv._1, kv._2._1)
            case None => (kv._1, kv._2._1)
            }
        })
      })

    // write to redis.
    val redis = new MyRedisClient("localhost", 6379)

    ssc.sparkContext.broadcast(redis)
    transformedRdd.foreachRDD(rdd => {
      rdd.foreach(record => {
        //val redis = new RedisClient("localhost", 6379)
        redis.lpush(record._1, record._2)
      }
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }
  */

}
