package FileFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.spark.SparkContext

/**
  * public static Configuration conf = new Configuration();
    public static String path = "/home/junius/data/output2";
    public static Path mapFile = new Path("path");
    public static void write() throws Exception {
        MapFile.Writer.Option keyClass = MapFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);
        MapFile.Writer writer=new MapFile.Writer(conf,mapFile,keyClass,valueClass);
        writer.append(new IntWritable(1),new Text("value1"));
        IOUtils.closeStream(writer);//关闭write流
    }
  */
object SequenceFileOps {
  def writeSequence(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(List(1,2,3))
    rdd.map(x => (x.toString, x)).saveAsSequenceFile("/home/junius/data/sequence")
  }

  def readSequence(sc: SparkContext): Unit = {
    val rdd = sc.sequenceFile[IntWritable, Text]("/home/junius/data/output", 4)
    rdd.foreach(strInt => println("" + strInt._1 + " " + strInt._2))
  }

  def readSequenceOne(sc: SparkContext): Unit = {
    val rdd = sc.sequenceFile[Text, LongWritable]("/home/junius/data/output", 4)

    /*
    rdd.mapPartitionsWithIndex((id, iter) => {
      println(id)
      while(iter.hasNext) iter.next()
      None.iterator
    })
    */

    rdd.foreachPartition(iter => {
      println("-------------")
      while (iter.hasNext) {
        val (key, value) = iter.next()
        println("" + key + " " + value)

      }
      println("**********")
    })
  }

  def writeMapfilePar(sc: SparkContext): Unit = {

    val rdd = sc.parallelize(List(1,2,3,4,5,6), 2).persist()

    rdd.map(i => (i.toString, i)).mapPartitionsWithIndex((id, iter) => {
      println(id)
      val writer = GetWriter.getWriter()
      while (iter.hasNext) {
        val (key, value) = iter.next()
        println("write " + key + " and " + value)
        writer.append(new IntWritable(value), new Text(key))
      }
      IOUtils.closeStream(writer)
      None.iterator
    })
  }

  def writeMapfile(sc: SparkContext): Unit = {

    val rdd = sc.parallelize(List(1,2,3,4,5,6), 2).persist()

    rdd.map(i => (i.toString, i)).foreachPartition(iter => {

      val writer = GetWriter.getWriter()
      while (iter.hasNext) {
        val (key, value) = iter.next()
        println("write " + key + " and " + value)
        writer.append(new IntWritable(value), new Text(key))
      }
      IOUtils.closeStream(writer)
    })

  }

  def main (args: Array[String]) {
    val sc = new SparkContext("local", "Simple App")

    //writeMapfilePar(sc)
    readSequenceOne(sc)
    sc.stop()

  }
}
