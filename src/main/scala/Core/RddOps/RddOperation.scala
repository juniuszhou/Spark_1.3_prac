package main.scala.Core.RddOps

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{TaskContext, SparkContext}

object RddOperation{
  def CallCompute(sc: SparkContext) = {
    val rdd = RddGenerator2.GenerateNumberRDD(sc)
    val par = rdd.partitions(0)

   // val res = rdd.compute(par, con)
   // res.foreach(println)
  }

  def CallDistinct(sc: SparkContext) = {
    val rdd = RddGenerator2.GenerateNumberRDD(sc)
    val res = rdd.distinct(4)
    res.foreach(println)
  }

  def CallMapPartition(sc: SparkContext) = {
    val rdd = RddGenerator2.GenerateNumberRDD(sc)
    def f = (i: Iterator[Int]) => i.map(x => x.toString)
    val res = rdd.mapPartitions(f)
    res.foreach(println)
  }

  /*
  def readTextFile(sc: SparkContext) = {
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
    val logData = sc.textFile(logFile, 4)
    val wordData: RDD[String] = logData.flatMap(i => i.split(" "))
    val letterData = logData.flatMap(line => {
      var all: Array[Char] = new Array[Char](line.length())
      
      (0 until (line.length())).map(i => (all.update(i, line.charAt(i))))
      all
      
    })
    letterData.map(println).count
  }
  
  def readNumbers(sc: SparkContext) = {
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/number.txt"
    val logData = sc.textFile(logFile, 4)
    logData.flatMap(i => i.split(" ")).map(println)
    logData
  }
  
  def wordCount(sc: SparkContext){
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
    val logData = sc.textFile(logFile, 4)
    val valcounts = logData.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _, 2)
  }
  

  def partitionSum(sc: SparkContext){
    val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/number.txt"
    val logData = sc.textFile(logFile, 4).cache
    //println(logData.map(num => num.toInt).reduce(_ + _).toString)
    val tmpData = logData.map(num => num.toInt).mapPartitionsWithIndex((i, nums) => nums.map(n => (i, n)), false)
    
    val tmp2Data = tmpData.reduceByKey(_ + _).map(i => { 
      println(i._1.toString + " partitionSum " + i._2)      
    }).count
   
    

    logData.map(num => num.toInt).mapPartitions(nums => { println("one part is over")
      nums.map(i => println( "junius " + i))      
      }).count

  }
  
  def myCoalesce(sc: SparkContext){
      val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/number.txt"
      val logData = sc.textFile(logFile, 3).cache
      val logData1 = logData.flatMap(i => {
        val a: Array[String] = new Array[String](10)
        (0 until 10).map(j => a(j) = i)
        a        
      })
      //logData.mapPartitions(items => items.map(str => str + " junius"), true)
      logData1.mapPartitionsWithIndex((i, nums) => nums.map(n => println(i.toString + " partitionSum " + n)), false).count
      
      val reLogData = logData1.coalesce(4, true)
      
      reLogData.mapPartitionsWithIndex((i, nums) => nums.map(n => println(i.toString + " partitionSum " + n)), false).count
  }
  
  def pairRddReduce(sc: SparkContext){
      val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
      val logData = sc.textFile(logFile, 3).cache
      val wordData = logData.flatMap(line => line.split(" ")).map(word => (word, 1))
      def s(i: Int, j: Int): Int =  i + j
      val result = wordData.combineByKey(n => n, s,s).map(counts => {
        println(counts._1 + " has " + counts._2)
      }).count
    
  }
  
  def pairCountByKey(sc: SparkContext){
      val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
      val logData = sc.textFile(logFile, 3).cache
      val wordData = logData.flatMap(line => line.split(" ")).map(word => (word, 1)).countByKey.map(counts => {
        println(counts._1 + " has " + counts._2)
      })    
  }
  
  def myCombineByKey(sc: SparkContext){
      val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
        
      //add tail to each word
      val logData = sc.textFile(logFile, 3).cache.flatMap(line => line.split(" ")).map(str => (str, str + " junius "))
      val log2 = logData.combineByKey[(Long, String)](
          //map value to tuple
          createCombiner = (s: String) => (1L, s),
          //merge init tuple and each value
          mergeValue = (c: (Long, String), v: String) => (c._1 + 2L, c._2 + v),
          //combine all partition of merger.
          mergeCombiners = (c1: (Long, String), c2: (Long, String)) => (c1._1 + c2._1, c1._2 + c2._2))
          
      log2.map(item => println(item._1 + " ij " + item._2._1 + " ji " + item._2._2)).count
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
          
      val log2 = logData.combineByKey[(Long, Array[Char])](
          //map value to tuple
          createCombiner = (s: Array[Char]) => (1L, s),
          //merge init tuple and each value
          mergeValue = (c: (Long, Array[Char]), v: Array[Char]) => (c._1 + 2L, c._2),
          //combine all partition of merger.
          mergeCombiners = (c1: (Long, Array[Char]), c2: (Long, Array[Char])) => (c1._1 + c2._1, c1._2))
          
      log2.map(item => println(item._1 + " ij " + item._2._1 + " ji " + item._2._2)).count
  }
 
 def myMapPartition(sc: SparkContext){
   val logFile = "/home/junius/git_hub/spark/examples/src/main/resources/news.txt"
   def f = (a: Iterator[String]) => { println(a.next + " myMapPartition")
     a
   }
   val logData = sc.textFile(logFile, 3).cache.mapPartitions(f, true).count
 }*/
 
  def main(args: Array[String]) {
	 val sc = new SparkContext("local[4]", "Simple App")
    CallMapPartition(sc);
    
    sc.stop

  }

}


