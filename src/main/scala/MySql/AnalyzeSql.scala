package main.scala.MySql

import MySql.TableGenerator
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SchemaRDD, catalyst, SQLContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Created by junius on 15-2-2.
 */
object AnalyzeSql {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.
    val sqlContext = new SQLContext(sc)


  }
}
