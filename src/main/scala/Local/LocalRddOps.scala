package Local

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object LocalRddOps{

  def wordCount(sc: SparkContext, data: RDD[String]){
    val counts = data.flatMap(line => line.split(" "))
    // counts.map(word => (word, 1)).reduceByKey(_ + _, 2).map(pair => println(pair._1 + " "  + pair._2)).count
  }

  def letterCount(sc: SparkContext, data: RDD[String]) = {
    val wordData: RDD[String] = data.flatMap(line => line.split(" "))
    val letterData = wordData.flatMap(word => {
      var all: Array[Char] = new Array[Char](word.length())
      (0 until (word.length())).map(i => (all.update(i, word.charAt(i))))
      all
    })
    // letterData.map(letter => (letter, 1)).reduceByKey(_ + _, 2).map(pair => println(pair._1 + " "  + pair._2)).count
  }

  def partitionSum(sc: SparkContext, data: RDD[String]){
    val tmpData = data.map(num => num.toInt).mapPartitionsWithIndex((i, nums) => nums.map(n => (i, n)), true)
    // val tmp2Data = tmpData.reduceByKey(_ + _, 3).map(i => {println(i._1.toString + " partitionSum " + i._2)}).count
  }

  def rddUnion(sc: SparkContext, data: RDD[String], data2: RDD[String]): Unit ={
    val unionData = data ++ data2
    val unionData2 = data.union(data2)
    unionData.map(println).count
    unionData2.map(println).count
  }

  def myAggregate(sc: SparkContext, data: RDD[String]): Unit ={
    // i j aggregate each partition, then m n aggregate all partitions' result
    val res = data.map(str => str.toInt).aggregate(0)((i, j) => {
      println(i.toString + " " + j.toString)
      i+j}, (m, n)=>{ println(m.toString + " xx " + n.toString)
      m+n})
    println(res.toString)
  }

  // you can see each data's index.
  // if data type is (a, b) tuple, you can not map( (a,b) => ). because just one parameter.
  // you shoud map(a => a._1 + a._2)
  def myZipWithIndex(sc: SparkContext, data: RDD[String]): Unit ={
    //data.zipWithIndex().map((str: String, l: Long) => str).count
    data.zipWithIndex().map( tup => println(tup._1 + " " + tup._2)).count
  }

  def myMapPartitionsWithIndex(sc: SparkContext, data: RDD[String]): Unit ={
    data.mapPartitionsWithIndex((i, items) => items.map(i.toString + " xx " + _)).map(println).count
  }

  def myCoalesce(sc: SparkContext, data: RDD[String]){
    val len = data.count
    val logData1 = data.flatMap(i => {
      val a: Array[String] = new Array[String](len.toInt)
      (0 until len.toInt).map(j => a(j) = i)
      a
    })
  }

  def mySimpleReduce(sc: SparkContext, data: RDD[String]): Unit ={
    println(data.map(_.toInt).reduce(_ + _))
  }
  def pairRddReduce(sc: SparkContext){
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
    val logData = sc.textFile(logFile, 3).cache
    val wordData = logData.flatMap(line => line.split(" ")).map(word => (word, 1))
    def s(i: Int, j: Int): Int =  i + j
    // val result = wordData.combineByKey(n => n, s,s).map(counts => {
    //  println(counts._1 + " has " + counts._2)
    // } ).count

  }

  def pairCountByKey(sc: SparkContext){
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
    val logData = sc.textFile(logFile, 3).cache
    //val wordData = logData.flatMap(line => line.split(" ")).map(word => (word, 1)).countByKey.map(counts => {
    //  println(counts._1 + " has " + counts._2)
    //})
  }

  def myCombineByKey(sc: SparkContext){
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"

    //add tail to each word
    val logData = sc.textFile(logFile, 3).cache.flatMap(line => line.split(" ")).map(str => (str, str + " junius "))
    /*val log2 = logData.combineByKey[(Long, String)](
      //map value to tuple
      createCombiner = (s: String) => (1L, s),
      //merge init tuple and each value
      mergeValue = (c: (Long, String), v: String) => (c._1 + 2L, c._2 + v),
      //combine all partition of merger.
      mergeCombiners = (c1: (Long, String), c2: (Long, String)) => (c1._1 + c2._1, c1._2 + c2._2))

    log2.map(item => println(item._1 + " ij " + item._2._1 + " ji " + item._2._2)).count*/
  }


  def myCombineByKey2(sc: SparkContext){
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"

    //add tail to each word
    val logData = sc.textFile(logFile, 3).cache.flatMap(line => line.split(" ")).map(
      str => (str, str.toList.flatMap(c => {
        val a: Array[Char] = new Array[Char](2)
        a(0) = c
        a(1) = ' '
        a
      }).toArray))
/*
    val log2 = logData.combineByKey[(Long, Array[Char])](
      //map value to tuple
      createCombiner = (s: Array[Char]) => (1L, s),
      //merge init tuple and each value
      mergeValue = (c: (Long, Array[Char]), v: Array[Char]) => (c._1 + 2L, c._2),
      //combine all partition of merger.
      mergeCombiners = (c1: (Long, Array[Char]), c2: (Long, Array[Char])) => (c1._1 + c2._1, c1._2))

    log2.map(item => println(item._1 + " ij " + item._2._1 + " ji " + item._2._2)).count */
  }

  def myMapPartition(sc: SparkContext){
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
    def f = (a: Iterator[String]) => { println(a.next + " myMapPartition")
      a
    }
    val logData = sc.textFile(logFile, 3).cache.mapPartitions(f, true).count
  }

  def main(args: Array[String]) {
    val path = "/home/junius/git_hub/spark_project/resource/"
    val numFile = path + "number.txt"
    val wordFile = path + "news.txt"

    val sc = new SparkContext("local[4]", "Simple App")
    val wordData = sc.textFile(wordFile, 3).cache
    val numData = sc.textFile(numFile, 3).cache

    // simple word and letter count
    //wordCount(sc, wordData)
    //letterCount(sc, wordData)

    // sum for each partition
    //partitionSum(sc, numData)

    //rddUnion(sc, numData, wordData)

    //myAggregate(sc, numData)
    //myZipWithIndex(sc, wordData)

    //myCoalesce(sc, numData)

    //myMapPartitionsWithIndex(sc, wordData)

    //mySimpleReduce(sc, numData)

    //how to call fold, result is 4045, three partition so 3000, then merge 1000.
    println(numData.map(_.toInt).fold(1000)(_ + _))

    //put all items in a partition into an array
    numData.glom().map(strs => strs.map(println))

    //val logData = readNumbers(sc)
    //join
    //val kvRDD1 = logData.map( i => (i, i))
    //val kvRDD2 = logData.map( i => (i, i + i)).join(kvRDD1)
    //kvRDD2.map(print).count

    sc.stop

  }
}


