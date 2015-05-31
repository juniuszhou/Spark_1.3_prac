package main.scala.MachineL

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.util.MLUtils

object MySVM {
  def main (args: Array[String]) {
    val dataFilePath = "/home/junius/develop/spark-1.3.0/data/mllib/sample_binary_classification_data.txt"
    val sc = new SparkContext("local", "Simple App")
    val data = MLUtils.loadLibSVMFile(sc, dataFilePath).cache()

    val updater = new SquaredL2Updater()
    val algorithm = new SVMWithSGD()
    algorithm.optimizer.setNumIterations(10)
    .setStepSize(0.0001)
    .setUpdater(updater)
    .setRegParam(1.0)

    val model = algorithm.run(data).clearThreshold()

    val predict = model.predict(data.map(_.features))
    predict.foreach(println(_))
    sc.stop()

  }
}
