package main.scala.MachineL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}

object MyPipeline {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Simple App")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._



    sc.stop()
  }
}
