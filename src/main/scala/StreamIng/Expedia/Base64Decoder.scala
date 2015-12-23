package StreamIng.Expedia

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.util.zip.GZIPInputStream

import org.apache.commons.codec.binary.Base64

import scala.util.Try

object Base64Decoder {
  def decodeUnzip(compressed: String): Try[String] = Try {
    val compressedByteArray = Base64.decodeBase64(compressed)
    val bis = new ByteArrayInputStream(compressedByteArray)
    val gis = new GZIPInputStream(bis)
    val br = new BufferedReader(new InputStreamReader(gis, "UTF-8"))
    br.readLine()
  }

  def main (args: Array[String]) {
    val a = List(1,2,3)
    a.flatMap(i => {
      if (i > 0) List(1,2) else Nil
    })
  }
}
