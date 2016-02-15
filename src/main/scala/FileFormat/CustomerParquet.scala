package FileFormat

import java.io.{File, FileReader, BufferedReader}
import java.util

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader, ParquetFileReader}
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.schema.MessageType

import scala.io.Source

/**
  * https://github.com/Parquet/parquet-compatibility/blob/master/parquet-compat/src/test/java/parquet/compat/test/ConvertUtils.java
  */
object CustomerParquet {
  def readFile(path: String): String = {
    val sb = new StringBuilder
    val ls = System.getProperty("line.separator")
    for (line <- Source.fromFile(path).getLines()) {
      sb.append(line)
      sb.append(ls)
    }
    sb.toString()
  }

  def getSchema(file: File): String = {
    val fileName = file.getName.substring(0, file.getName.length - ".csv".length) + ".schema"

    val schemaFile = new File(file.getParentFile, fileName)
    Schema.create(Schema.Type.STRING)
    readFile(schemaFile.getAbsolutePath)
  }

  def readFile() = {
    val inPath = "/home/junius/data/parquet/one.par"
    val config = new Configuration(true)
    val readSupport: GroupReadSupport = new GroupReadSupport()
    val readFooter = ParquetFileReader.readFooter(config, new Path(inPath))
    val schema = readFooter.getFileMetaData.getSchema

    val reader = new ParquetReader(new Path(inPath), readSupport)
    val g = reader.read()
    (0 until schema.getFieldCount).foreach(i => {
      val str = g.getValueToString(i, 0)
      println(str)
    })
  }

  def writeFile() = {
    val csvFile = new File("/home/junius/mygit/Spark_1.3_prac/src/main/scala/FileFormat/customer.csv")
    val rawSchema = getSchema(csvFile)

    val outPath: String = "/home/junius/data/parquet/one.par"

    val schema: MessageType = MessageTypeParser.parseMessageType(rawSchema)
    val writer = new CsvParquetWriter(new Path(outPath), schema)

    val data = new util.ArrayList[String]()
    data.add("1")
    data.add("1")
    writer.write(data)

    writer.close()
  }

  def main(args: Array[String]): Unit = {

readFile()
  }
}
