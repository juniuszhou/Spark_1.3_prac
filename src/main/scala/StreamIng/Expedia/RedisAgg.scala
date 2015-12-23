package StreamIng.Expedia

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Timer {
  def apply(interval: Int, repeats: Boolean = true)(op: => Unit) {
    val timeOut = new javax.swing.AbstractAction() {
      def actionPerformed(e : java.awt.event.ActionEvent) = op
    }
    val t = new javax.swing.Timer(interval, timeOut)
    t.setRepeats(repeats)
    t.start()
  }
}

object RedisAgg {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("spark://10.2.68.60:7077")
      .setAppName("LpasTrends")

    val sc = new SparkContext(sparkConf)

    Timer(6000){
      println("_______________")
      val keysRdd = sc.fromRedisKeyPattern(("localhost", 6379), "*", 2)
      val kvRdd = keysRdd.getKV()
      val hotelIds = kvRdd.map(str => {
        val items = str._1.split(":")
        val hotelId = items(3)
        val tpid = items(5)
        val date = items(7)
        val value = str._2
        val price =  JSON.parseFull(str._2).get.asInstanceOf[Map[String, Object]].get("price").get.toString.toFloat


        (hotelId + ":" + tpid, (price, value))
      })

      val lowestPriceRdd = hotelIds.groupBy(_._1).map(kvs => {
        val values = kvs._2
        var pv = (Float.MaxValue, "")
        values.map(_._2).foreach(strs => if (pv._1 > strs._1.toFloat) pv = strs)
        (kvs._1, pv._2)
      })
      sc.toRedisKV(lowestPriceRdd, ("localhost", 6379))

    }

    while(true){

    }
  }
}
