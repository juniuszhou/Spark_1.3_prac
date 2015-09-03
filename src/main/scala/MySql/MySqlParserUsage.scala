package MySql

import org.apache.spark.sql.catalyst.{SqlLexical, SqlParser}

/**
 * Created by junius on 15-1-25.
 */
object MySqlParserUsage {
  def main (args: Array[String]) {

    /*val lexical = new SqlLexical(Seq()) // Seq("SELECT", "FROM"))
    // val lexical = new StdLexical()  // can't deal with character like +
    var scanner = new lexical.Scanner("SELECT name, c1+c2, id FROM records")

    while (!scanner.atEnd) {
      println(scanner.first.chars)
      scanner = scanner.rest
    }
    //println(tokens.toString)


    case class Keyword(str: String)

    //junius, implicit keyword to parser.

    val sp: SqlParser = new SqlParser
    // sp.phrase[LogicalPlan]()

    val cmd = sp("SELECT COUNT(*) FROM records")
    println(cmd.toString)

    val cmd2 = sp("SELECT name, id FROM records")
    println(cmd2.toString)



    // this is not supported in spark 1.2
    // val cmd3 = sp("SET name=2 ")
    // println(cmd3.toString)

    // 4、hive不支持INSERT INTO, UPDATE, DELETE操作
    //val cmd3 = sp("INSERT INTO records VALUES(1,2)")
    //println(cmd3.toString)

    */
  }

}
