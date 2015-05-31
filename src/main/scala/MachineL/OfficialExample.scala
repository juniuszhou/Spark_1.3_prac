package main.scala.MachineL

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}

case class LabeledDocument(id: Long, text: String, label: Double)
case class Document(id: Long, text: String)

object OfficialExample {
  def main (args: Array[String]) {

    val sc = new SparkContext("local", "pipeline")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def myEstimator() = {
      val training = sc.parallelize(Seq(
        LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
        LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
        LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
        LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5))))

      // Create a LogisticRegression instance.  This instance is an Estimator.
      val lr = new LogisticRegression()
      // Print out the parameters, documentation, and any default values.
      println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

      // We may set parameters using setter methods.
      lr.setMaxIter(10)
        .setRegParam(0.01)

      // Learn a LogisticRegression model.  This uses the parameters stored in lr.
      val model1 = lr.fit(training.toDF)
      // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
      // we can view the parameters it used during fit().
      // This prints the parameter (name: value) pairs, where names are unique IDs for this
      // LogisticRegression instance.
      println("Model 1 was fit using parameters: " + model1.fittingParamMap)

      // We may alternatively specify parameters using a ParamMap,
      // which supports several methods for specifying parameters.
      val paramMap = ParamMap(lr.maxIter -> 20)
      paramMap.put(lr.maxIter, 30) // Specify 1 Param.  This overwrites the original maxIter.
      paramMap.put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

      // One can also combine ParamMaps.
      val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name
      val paramMapCombined = paramMap ++ paramMap2

      // Now learn a new model using the paramMapCombined parameters.
      // paramMapCombined overrides all parameters set earlier via lr.set* methods.
      val model2 = lr.fit(training.toDF, paramMapCombined)
      println("Model 2 was fit using parameters: " + model2.fittingParamMap)

      // Prepare test data.
      val test = sc.parallelize(Seq(
        LabeledPoint(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
        LabeledPoint(0.0, Vectors.dense(3.0, 2.0, -0.1)),
        LabeledPoint(1.0, Vectors.dense(0.0, 2.2, -1.5))))

      // Make predictions on test data using the Transformer.transform() method.
      // LogisticRegression.transform will only use the 'features' column.
      // Note that model2.transform() outputs a 'myProbability' column instead of the usual
      // 'probability' column since we renamed the lr.probabilityCol parameter previously.
      model2.transform(test.toDF)
        .select("features", "label", "myProbability", "prediction")
        .collect()
        .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println("($features, $label) -> prob=$prob, prediction=$prediction")
      }
    }

    def myPipeline() = {
      // Prepare training documents, which are labeled.
      val training = sc.parallelize(Seq(
        LabeledDocument(0L, "a b c d e spark", 1.0),
        LabeledDocument(1L, "b d", 0.0),
        LabeledDocument(2L, "spark f g h", 1.0),
        LabeledDocument(3L, "hadoop mapreduce", 0.0)))

      // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
      val hashingTF = new HashingTF()
        .setNumFeatures(1000)
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
      val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.01)
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, lr))

      // Fit the pipeline to training documents.
      val model = pipeline.fit(training.toDF())

      // Prepare test documents, which are unlabeled.
      val test = sc.parallelize(Seq(
        Document(4L, "spark i j k"),
        Document(5L, "l m n"),
        Document(6L, "mapreduce spark"),
        Document(7L, "apache hadoop")))

      // Make predictions on test documents.
      model.transform(test.toDF())
        .select("id", "text", "probability", "prediction")
        .collect()
        .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println("($id, $text) --> prob=$prob, prediction=$prediction")
      }
    }

    def myCrossValidation() = {
      val training = sc.parallelize(Seq(
        LabeledDocument(0L, "a b c d e spark", 1.0),
        LabeledDocument(1L, "b d", 0.0),
        LabeledDocument(2L, "spark f g h", 1.0),
        LabeledDocument(3L, "hadoop mapreduce", 0.0),
        LabeledDocument(4L, "b spark who", 1.0),
        LabeledDocument(5L, "g d a y", 0.0),
        LabeledDocument(6L, "spark fly", 1.0),
        LabeledDocument(7L, "was mapreduce", 0.0),
        LabeledDocument(8L, "e spark program", 1.0),
        LabeledDocument(9L, "a e c l", 0.0),
        LabeledDocument(10L, "spark compile", 1.0),
        LabeledDocument(11L, "hadoop software", 0.0)))

      // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
      val hashingTF = new HashingTF()
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
      val lr = new LogisticRegression()
        .setMaxIter(10)
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, lr))

      // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
      // This will allow us to jointly choose parameters for all Pipeline stages.
      // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
      val crossval = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator)
      // We use a ParamGridBuilder to construct a grid of parameters to search over.
      // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
      // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
      val paramGrid = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
        .addGrid(lr.regParam, Array(0.1, 0.01))
        .build()
      crossval.setEstimatorParamMaps(paramGrid)
      crossval.setNumFolds(2) // Use 3+ in practice

      // Run cross-validation, and choose the best set of parameters.
      val cvModel = crossval.fit(training.toDF)

      // Prepare test documents, which are unlabeled.
      val test = sc.parallelize(Seq(
        Document(4L, "spark i j k"),
        Document(5L, "l m n"),
        Document(6L, "mapreduce spark"),
        Document(7L, "apache hadoop")))

      // Make predictions on test documents. cvModel uses the best model found (lrModel).
      cvModel.transform(test.toDF)
        .select("id", "text", "probability", "prediction")
        .collect()
        .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
    }

    sc.stop()
  }
}
