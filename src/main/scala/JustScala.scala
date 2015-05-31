class JustScala{
  val str = "hello"
}
object JustScala {
  def main (args: Array[String]) {

    def sum(a: Int, b: Int)(implicit c:Int) : Int = {
      
      a + b + c
    }
    implicit val c = 0
    println(sum(1,2))
  }
}
