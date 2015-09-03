package MachineLearning

import breeze.collection.mutable.SparseArray
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors, SparseVector}
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by junius on 14-11-17.
 */
object MlBasicDataType {
  def main (args: Array[String]) {
    // the basic data type from breeze.
    // val numRows : scala.Int, val numCols : scala.Int, val values : scala.Array[scala.Double]
    val indexArray = Array(1,2,3,4,5,6)
    val dataArray = Array(1.0,2.0,3.0,4.0,5.0,6.0)

    // dense matrix and not init it. just give the dimension.
    val bldm = new  breeze.linalg.DenseMatrix[Double](2,3)

    // sparse vector with data.
    // val blsm = new SparseVector[Double](indexArray, dataArray, 100)

    // dense vector
    val bldv = DenseVector.zeros[Double](10)

    // sparse array just give the size.
    val blsa = new SparseArray[Double](100)

    //
    // val blsm = new CSCMatrix[]()


    //basic data type used in mllib
    // val dv: Vector = Vectors.dense(1.0, 2.0, 3.0)

    // first array is index. second array is corresponding value.
    // val sv1: Vector = Vectors.sparse(4, Array(0,1), Array(1.0, 3.0))
    val pos: LabeledPoint = LabeledPoint(1.0, Vectors.dense(0, 1, 2))

    // dense matrix defined in mllib
    val dm: DenseMatrix = new DenseMatrix(2, 3, dataArray)
  }
}
