

class a {}
class b extends a {}

object JustScala {
  def main (args: Array[String]) {


    def get(): List[b] = {
      List(new a).asInstanceOf[List[b]]
    }

    println(Math.pow(1.1, 10))
  }
}
