package MySql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

//case class Person(name: String, age: Int)


object firstSparkSql {
  def PrintTable(sqlContext: SQLContext) = {
    // or we can import sqlContext._ then we can use sql without domain name.
    import sqlContext._
    sqlContext.sql("SELECT * FROM MyRecords").collect.foreach(println)

    // select column.
    val rdd = sqlContext.sql("SELECT id, name FROM MyRecords")

    // where order by
    //rdd.where('id === "junius").orderBy('id.asc).printSchema()

    // rdd.printSchema()

  }

  /*def GetRDDFromSql(sqlContext: SQLContext) : RDD[_]= {
    // or we can import sqlContext._ then we can use sql without domain name.
    val rdd = sqlContext.sql("SELECT * FROM MyNumberPairs")
    rdd.foreach(println)
    println("-------------------------------------------")
    rdd.printSchema()
    rdd
  }
*/
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.dialect", "sql")
    TableGenerator.GenerateTable(sqlContext)
    TableGenerator.GenerateIntTable(sqlContext)

    // PrintTable(sqlContext)
    PrintTable(sqlContext)
  }
}