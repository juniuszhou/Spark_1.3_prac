package main.scala.MachineL.fpm

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}

/** it is Aprior algorithm implementation.
 * based on the transaction, to find out the order of items.
 * in output, for each rule, antecedent is a item set, consequence is other item set.
 * the rule indicates customer will buy consequence after antecedent based on the confidence.
  * confidence means the percentage of candidate item set divided by whole transaction number.
 */
object MyAssociationRules {
  def main (args: Array[String]) {
    val sc = new SparkContext("local[2]", "Simple App")

    val freqItemsets = sc.parallelize(Seq(
      (Set("s"), 3L), (Set("z"), 5L), (Set("x"), 4L), (Set("t"), 3L), (Set("y"), 3L),
      (Set("r"), 3L),
      (Set("x", "z"), 3L), (Set("t", "y"), 3L), (Set("t", "x"), 3L), (Set("s", "x"), 3L),
      (Set("y", "x"), 3L), (Set("y", "z"), 3L), (Set("t", "z"), 3L),
      (Set("y", "x", "z"), 3L), (Set("t", "x", "z"), 3L), (Set("t", "y", "z"), 3L),
      (Set("t", "y", "x"), 3L),
      (Set("t", "y", "x", "z"), 3L), (Set("t", "x", "s"), 4L)
    ).map {
      case (items, freq) => new FPGrowth.FreqItemset(items.toArray, freq)
    })

    val ar = new AssociationRules()

    val results1 = ar
      .setMinConfidence(1.0)
      .run(freqItemsets)
      .collect()

    results1.foreach(str => {
      val ant = str.antecedent
      val con = str.consequent
      println("***************")
      ant.foreach(println(_))
      println("_______________")
      con.foreach(println(_))
      println("***************")
    })
  }
}
