package MachineLearning

import breeze.linalg.DenseVector

/**
 * Created by junius on 14-11-9.
 */

object MlLocalBreeze {
  def main (args: Array[String]) {
    // breeze dence vector
    val data: Array[Int] = new Array[Int](10)
    (0 until 10).foreach(i => data.update(i, i))
    val v1 = new DenseVector(data)
    val v2 =  new DenseVector(data)
    println(v1.toString())
    println((v1 + v2).toString())
    // println((v1 * v2).toString())
    //

  }
}
