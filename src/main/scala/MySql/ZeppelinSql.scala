package main.scala.MySql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SchemaRDD, catalyst, SQLContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.functions._

object ZeppelinSql {
  case class Student(id: Int, name: String, age: Int)
  def main (args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("simple sql")
    conf.setMaster("yarn-client")
    conf.set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // An existing SparkContext.

    val sqlContext = new SQLContext(sc)
    val text = sc.textFile("s3://growth-data-analysis/jun/students.csv", 4)
    val rdd = text.map(str => str.split(","))
      .map(triple => Student(triple(0).toInt, triple(1), triple(2).toInt))

    import sqlContext.implicits._
    val note = rdd.toDF()
    note.registerTempTable("Students")


  }
}
