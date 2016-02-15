package MachineL.Regression

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.regression.AFTSurvivalRegression
import org.apache.spark.mllib.linalg.Vectors
// $example off$
import org.apache.spark.sql.SQLContext

object AftRegression {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AFTSurvivalRegressionExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val training = sqlContext.createDataFrame(Seq(
      (1.218, 1.0, Vectors.dense(1.560, -0.605)),
      (2.949, 0.0, Vectors.dense(0.346, 2.158)),
      (3.627, 0.0, Vectors.dense(1.380, 0.231)),
      (0.273, 1.0, Vectors.dense(0.520, 1.151)),
      (4.199, 0.0, Vectors.dense(0.795, -0.226))
    )).toDF("label", "censor", "features")

    //split the whole training set into three parts. data in array should keep order.
    val quantileProbabilities = Array(0.3, 0.6)


    val aft = new AFTSurvivalRegression()
      .setQuantileProbabilities(quantileProbabilities)
      .setQuantilesCol("quantiles")

    val model = aft.fit(training)

    // Print the coefficients, intercept and scale parameter for AFT survival regression
    println(s"Coefficients: ${model.coefficients} Intercept: " +
      s"${model.intercept} Scale: ${model.scale}")
    model.transform(training).show(false)
    // $example off$

    sc.stop()
  }
}
