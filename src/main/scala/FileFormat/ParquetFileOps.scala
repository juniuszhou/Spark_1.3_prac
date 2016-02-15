package FileFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.schema.MessageType


object ParquetFileOps {
  def main(args: Array[String]) {
    val path = "/home/junius/data/parquet/par_0"
    val conf = new Configuration()
    val schema = new MessageType("table")

    val pfw = new ParquetFileWriter(conf, schema, new Path(path))
    // pfw.writeDataPage(100, 100)



  }
}
