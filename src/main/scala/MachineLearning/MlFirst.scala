package MachineLearning

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

object MlFirst {
	def printPoints(l: LabeledPoint) = {
		val d: Double = l.label
				val v = l.features
				println(d.toString + " feature value as " + v.toString())
	}

	def generateRDDVector(rl: RDD[LabeledPoint]){
		/* val rows: RDD[Vector] = rl.map(l => l.features)

				//distributes matrix
		//		val mat: RowMatrix = new RowMatrix(rows)
		val m = mat.numRows()
		val n = mat.numCols()
		println(m.toString + " and " + n.toString)

		println(mat.computeColumnSummaryStatistics().mean)
		println(mat.computeColumnSummaryStatistics().variance)
		println(mat.computeColumnSummaryStatistics().count) */
	}

	def mySvm(rl: RDD[LabeledPoint]) = {
		val splits = rl.randomSplit(Array(0.8, 0.2), seed = 111l)
				val training = splits(0).cache()
				val test = splits(1).cache()

				val model = SVMWithSGD.train(training, 100)
				model.clearThreshold()
				println("junius junius   ")
				test.map( p => println(p.label.toString + " but " + model.predict(p.features))).count()

	}

	def myLogistic(rl: RDD[LabeledPoint]) = {
		val splits = rl.randomSplit(Array(0.8, 0.2), seed = 111l)
				val training = splits(0).cache()
				val test = splits(1).cache()

				val model = LogisticRegressionWithSGD.train(training, 100)
				model.clearThreshold()
				println("junius junius   ")

				test.map( p => println(p.label.toString + " but " + model.predict(p.features))).count()
	}

	def myRegression(rl: RDD[LabeledPoint]) = {
		val splits = rl.randomSplit(Array(0.8, 0.2), seed = 111l)
				val training = splits(0).cache()
				val test = splits(1).cache()

				val model = org.apache.spark.mllib.regression.LinearRegressionWithSGD.train(training, 100)
				test.map( p => println(p.label.toString + " but " + model.predict(p.features))).count()    
	}

	def myAls(sc: SparkContext) = {
		val data = sc.textFile("/home/junius/develop/spark-1.0.0/mllib/data/als/test.data")
				val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
				Rating(user.toInt, item.toInt, rate.toDouble)
				})

				// Build the recommendation model using ALS
				val rank = 10
				val numIterations = 20
				val model = ALS.train(ratings, rank, numIterations, 0.01)

				// Evaluate the model on rating data
				val usersProducts = ratings.map { case Rating(user, product, rate) =>
				(user, product)
		}
		val predictions = 
				model.predict(usersProducts).map { case Rating(user, product, rate) => 
				((user, product), rate)
		}
		val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
		((user, product), rate)}


		var MSE: Double = 0.0
				val map1 = predictions.toArray.toMap
				map1(1,2)

		ratesAndPreds.map(item => {  
			println(map1(item._1) + " but " + item._2)
			val disc = (map1(item._1) - item._2)
			MSE += (if (disc > 0) disc else -disc)
			println(MSE)
		}).count()

		println(MSE)
		println("Mean Squared Error = " + MSE.toString)
	}

	def main(args: Array[String]) {
    val dataFilePath = "/home/junius/develop/spark-1.1.0/data/mllib/sample_svm_data.txt"
		val sc = new SparkContext("local", "Simple App")
		val data = sc.textFile(dataFilePath)
		val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))




		//to support the data format defined in libsvm and liblinear.
		val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, dataFilePath)

		generateRDDVector(examples)
		mySvm(examples)
		myAls(sc)

	}
}
