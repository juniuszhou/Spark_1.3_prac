package FileFormat

import java.io.{FileReader, BufferedReader, File}

import org.apache.avro.Schema
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

/**
  * http://blog.cloudera.com/blog/2014/05/how-to-convert-existing-data-into-parquet/
  */
object convertCsvToParquet {
  def readFile(path: String): String = {
    val reader = new BufferedReader(new FileReader(path))
    val sb = new StringBuilder

    val ls = System.getProperty("line.separator")
    var line: String = null

    while ((line = reader.readLine()) != null ) {
      sb.append(line)
      sb.append(ls)
    }
    reader.close()

    sb.toString()

  }

  def getSchema(file: File): String = {
    val fileName = file.getName.substring(
      0, file.getName.length - ".csv".length) + ".schema"

    val schemaFile = new File(file.getParentFile, fileName)
    Schema.create(Schema.Type.STRING)
    readFile(schemaFile.getAbsolutePath)
  }

  def main(args: Array[String]) {
    val schema = Schema.create(Schema.Type.STRING)
    val writer = new AvroParquetWriter(
      new Path("/home/junius/data/parquet/one.par"), schema)
    writer.write("ok".asInstanceOf[Nothing])
    writer.close()
  }
}
