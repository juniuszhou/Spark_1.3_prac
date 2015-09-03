package MySql

import main.scala.RddGenerator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random


/**
 * Created by junius on 15-1-22.
 */
case class MyParquetRecord(id: Int, age: Int)
object MyParquet {
  // seems there is connection between case class and Product.
  //here path is a directory of Parquet.
  def SaveRDDAsParquet[A <: Product: TypeTag](rdd: RDD[A], path: String, sqlContext: SQLContext) = {
    //import sqlContext.createSchemaRDD
    //rdd.saveAsParquetFile(path)

  }

  // since Parquet data file includes its schema itself. so spark doesn't need apply schema to it.
  def ReadRDDFromParquet(sqlContext: SQLContext) = {
    val path = "/home/junius/develop/spark-1.2.0/examples/src/main/resources/users.parquet"
    val people = sqlContext.parquetFile(path)
    println(people)

    people.registerTempTable("people")
  }
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.
    val sqlContext = new SQLContext(sc)

    // save random generated data into parquet file.
    val r = new Random()
    val data = (0 to 10).map(i => MyParquetRecord(r.nextInt, r.nextInt))
    val rdd = sqlContext.sparkContext.parallelize(data)
    SaveRDDAsParquet(rdd, "/home/junius/stu.txt", sqlContext)

    // read RDD from parquet file
    ReadRDDFromParquet(sqlContext)
    sqlContext.sql("SELECT * FROM people").map(row => println(row.toString())).count

    sc.stop()
  }
}
