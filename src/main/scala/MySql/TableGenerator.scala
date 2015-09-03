package MySql

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Random

// only case class RDD can be converted to schema RDD.
// the Java reflect map the data member to column in table of SQL.
case class MyRecord(id: String, name: String)
case class MyNumPair(f: Int, s: Int)
/* class MyRecord{}
object MyRecord{
  def apply(s: String, s2: String) = new MyRecord
}*/

object TableGenerator {
  def GenerateTable(sparkContext: SparkContext, catalog: Catalog) = {

    val r = new Random()

    val data = (0 to 10).map(i => MyRecord(r.nextString(6), r.nextString(12)))
    val rdd = sparkContext.parallelize(data)

    // only case class RDD can call register temp talbe.
    //catalog.registerTable(None, "MyRecords")
    // rdd.registerTempTable("MyRecords")
    // other way to register table.
    // sqlContext.registerRDDAsTable(rdd, "Record")
  }

  def GenerateTable(sqlContext: SQLContext) = {
    import sqlContext._
    // must import all method in sqlContext. then we can call implicit defined
    // in this class. we can get schema RDD from case class RDD.
    val r = new Random()

    val data = (0 to 10).map(i => MyRecord(r.nextString(6), r.nextString(12)))
    val rdd = sqlContext.sparkContext.parallelize(data)

    // only case class RDD can call register temp talbe.
    //rdd.registerTempTable("MyRecords")
    // other way to register table.
    // sqlContext.registerRDDAsTable(rdd, "Record")
  }

  def GenerateIntTable(sqlContext: SQLContext) = {
    import sqlContext._
    // must import all method in sqlContext. then we can call implicit defined
    // in this class. we can get schema RDD from case class RDD.
    val r = new Random()

    val data = (0 to 10).map(i => MyNumPair(r.nextInt, r.nextInt))
    val rdd = sqlContext.sparkContext.parallelize(data)

    // only case class RDD can call register temp talbe.
    //rdd.registerTempTable("MyNumberPairs")
    // other way to register table.
    // sqlContext.registerRDDAsTable(rdd, "Record")
  }

  //create table without case class.
  def GenerateTableWithSchema(sqlContext: SQLContext) = {
    // import sqlContext._

    // create a schema via set the field name.
    val schema = StructType("f s".split(" ").map(
      fieldName => StructField(fieldName, StringType, true)))

    val r = new Random()
    // generate data like MyNumPair
    val data = (0 to 10).map(i => r.nextInt.toString + "," + r.nextInt)
    val rdd = sqlContext.sparkContext.parallelize(data)

    // transform to Row RDD
    val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // combine the schema and data.  before here, row RDD and schema don't know each other.
    val pairSchemaRDD = sqlContext.applySchema(rowRDD, schema)
    pairSchemaRDD.registerTempTable("MyDesignedTable")
  }

  // test the API provided for other class.
  def main (args: Array[String]) {
    val sc = new SparkContext("local[4]", "Simple App") // An existing SparkContext.
    val sqlContext = new SQLContext(sc)
    GenerateTableWithSchema(sqlContext)
  }
}
