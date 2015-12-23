package main.scala.MachineL

import MyUtil.RddGenerator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}

object MyDataFrame {
  def ScanDataFrame(data: DataFrame ): Unit = {
    //print column names
    data.columns.foreach(colName => println("column name is: ") + colName)

    //print all data in first column.
    data.foreach(row => println(row.getString(0)))
    // data.foreach(row => row.get)
  }

  def ScanRdd(data: RDD[_]): Unit = {
    data.foreach(println)
  }

  def main (args: Array[String]) {
    val sc = new SparkContext("local", "Simple App")
    val data = RddGenerator.GenerateTableString(sc)

    val sqlContext = new SQLContext(sc)
    // import sqlContext.implicits._  unnecessary to include those implicits.

    //transform from RDD[Product] to data frame.
    val frame = sqlContext.createDataFrame(data)

    // get inner rdd from frame.
    val rdd = frame.rdd


    ScanDataFrame(frame)
    ScanRdd(rdd)
  }
}
