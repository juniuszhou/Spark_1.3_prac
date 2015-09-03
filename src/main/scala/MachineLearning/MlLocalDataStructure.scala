package MachineLearning



import java.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object MlLocalDataStructure {
  /*
	def printPoints(l: LabeledPoint) = {
		val d: Double = l.label
				val v = l.features
				println(d.toString + " feature value as " + v.toString())
	}

  def BlasData = {
    //all methods defined in BLAS are mllib private
    //val dv1: Vector = Vectors.dense(1.0, 2.0, 3.0)
    //val dv2: Vector = Vectors.dense(1.0, 2.0, 3.0)
  }

  def DenseMatrixOps = {
    //define a dense matrix, note there is no sparse matrix yet in spark
    val dm: Matrix = Matrices.dense(2,2,Array(1.0, 2.0, 3.0, 4.0))
    println(dm.toString())
  }

  def RowMatrixOps = {
    def ArrayToVector(t: Array[Double]): Vector = {
      val x = Vectors.dense(t)
      x
    }

    val sc = new SparkContext("local[4]", "MlLocalDataStructure")
    val dataSet: Array[Double] = new Array[Double](25)
    val random = new Random()
    (0 until 25).map(index => (dataSet(index) = random.nextDouble()))

    val dataRdd: RDD[Double] = sc.makeRDD(dataSet, 10).cache
    val v: Vector = ArrayToVector(dataRdd.toArray())

    //RowMatrix is @Experimental now

    // val rm = new RowMatrix(v, 5, 5)
  }

  // CoordinateMatrix IndexedRowMatrix

  def denseVectorOps = {
    //init a vector from list
    val dv: Vector = Vectors.dense(1.0, 2.0, 3.0)
    dv.toArray.map(println)
  }

  def SparseVectorOps = {
    //init a vector from list
   // val sv: Vector = Vectors.sparse(4, Array(0,1), Array(1.0, 3.0))
    //sv.toArray.map(println)

  }

  def LabelPointOps = {
    val pos: LabeledPoint = LabeledPoint(1.0, Vectors.dense(0, 1, 2))
    println(pos.label)
    println(pos.features)
  }

  /*
  def GenerateSVMData: RDD[LabeledPoint] = {
    LabeledPoint
  }
  */

	def main(args: Array[String]) {
		val sc = new SparkContext("local", "object MlLocalDataStructure")

		//to support the data format defined in libsvm and liblinear.
		//val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "/home/junius/develop/spark-1.1.0/data/mllib/sample_libsvm_data.txt")
    //examples.count()
    //val result = examples.take(10)
    // println(result.length)
    //result.foreach(i => println(i.toString))

    result.foreach(f => {
      print(f.label + " ")
      //f.features.toArray.foreach(f2 => print(f2 + " "))
      println

    })


    //denseVectorOps
	}*/
}

