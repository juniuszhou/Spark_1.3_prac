

class a {}
class b extends a {}

object JustScala {
  def main (args: Array[String]) {


    val ll = Seq(1,2,3,4)
    val res = ll.zipWithIndex
    res.foreach(println)
  }
}
