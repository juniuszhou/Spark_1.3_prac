package main.scala.StreamIng

import org.apache.spark.Logging


object KafkaWordcount extends Logging {
  def main(args: Array[String]) {
    if (args.length < 7) {
      System.err.println("Usage: KafkaWordCount <kafkaZkQuorum>, <group>, <topics>, <numPartitions>, <numThreads>, <hbaseZkQuorum>, <hbaseMonitoringTable>")
      System.exit(1)
    }

    val Array(kafkaZkQuorum, group, topics, numPartitions, numThreads, hbaseZkQuorum, hbaseMonitoringTable) = args

    val sparkConf = new SparkConf()
      .setAppName("KafkaWordCount")

    SparkContext.jarOfClass(this.getClass).map { x =>
      sparkConf.setJars(Seq(x))
    }


    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val lines = ssc.union((1 to numPartitions.toInt).map {
      x =>
        SparkUtils.createStreamPartition(ssc, KafkaStreamConsumerConfig(x,
          numPartitions.toInt,
          numThreads.toInt,
          group,
          kafkaZkQuorum,
          topics.split(","),
          100000,
          1024 * 1024,
          StorageLevel.MEMORY_AND_DISK_SER_2,
          hbaseZkQuorum,
          hbaseMonitoringTable,
          true,
          false)).map(x => x._2)
    })

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

