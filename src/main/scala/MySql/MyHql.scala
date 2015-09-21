package main.scala.MySql

import MySql.TableGenerator
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/*
  sbt/sbt -Phive assembly
  re-build spark if hive not included.
 */
object MyHql {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.

    val hiveContext = new HiveContext(sc)

    hiveContext.setConf("spark.sql.dialect", "hql")
    TableGenerator.GenerateTable(hiveContext)
    TableGenerator.GenerateIntTable(hiveContext)

  }
}
