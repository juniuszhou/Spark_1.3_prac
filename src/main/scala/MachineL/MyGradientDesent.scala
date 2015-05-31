package main.scala.MachineL
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}

/**
 * Created by junius on 15-5-24.
 */
object MyGradientDesent {
  abstract class MyGradient {
    def compute(data: Array[Double], label: Double, weights: Array[Double]): (Array[Double], Double)

  }

  object MyHingeGradient extends MyGradient{
    //label here could be -1 or 1
    def compute(data: Array[Double], label: Double, weights: Array[Double]): (Array[Double], Double) = {

      var sum = 0.0
      (0 until data.length).foreach(i => sum += data(i) * weights(i))
      if(sum * label > 1) {
        // if greater than 1, means the point is correctly classified otherwise it is negative.
        // and it also should be greater than 1, not within support vector
        println(" compute empty called.")
        (Array.empty, 0.0)
      } else {
        (0 until data.length).foreach(i => data(i) * label)
        // gradient should be data self and times with label to guarantee it is positive
        // loss is gap the distance with 1
        (data, 1.0 - label * sum)
      }
    }
  }

  def MySvm() = {
    val maxIteration = 10
    val rate = 0.01 // learning speed
    val reg = 0.1 // regularization parameter

    //we just declare one training point. label is -1 or 1. and three features.
    val data: Array[(Double, Array[Double])] = Array((-1.0, Array(1.0, 2.0, 3.0)))

    val weight = Array[Double](0.0, 0.0, 0.0) // the length of weight should be 3, the same with features.

    def train() = {
      // transform at first.
      // according to mllib, scale weight also can transform training.
      val mean = new Array[Double](3) // three features

      data.foreach(lArr => {
        val arr = lArr._2
        (0 until mean.length).foreach(i => mean(i) += arr(i))
      })
      (0 until mean.length).foreach(i => mean(i) /= data.length)

      val std = new Array[Double](3)
      data.foreach(lArr => {
        val arr = lArr._2
        (0 until mean.length).foreach(i => std(i) += Math.pow(arr(i) - mean(i), 2.0))
      })
      (0 until mean.length).foreach(i => std(i) /= data.length)

      // transform data
      val normedData = data.map(lArr => (lArr._1, {
        val arr = lArr._2
        val res = new Array[Double](arr.length)
        (0 until arr.length).foreach(i => res(i) = ( arr(i) - mean(i) ) * std(i))
        res
      }))

      // consider the intercept
      val useIntercept = true

      // loop
      var index = 1
      for (index <- 1 to maxIteration){
        //compute gradient
        val gradient = normedData.map(dArr =>
          MyHingeGradient.compute(dArr._2, dArr._1, weight))
          .map(arr => arr._1).map(arr => arr.sum)

        // compute loss by add regularization
        // weight - rate * (gradient + reg * w) // then we get new weight.
        println("weigth len is", weight.length, " gradient len is ", gradient.length)
        (0 until weight.length).foreach(i => weight(i) = weight(i) - rate * (gradient(0) + reg * weight(i)))

        // print out the weight
        print(index + " round: ")
        weight.foreach(d => print(", " + d))
        println()
      }
    }

    train()
  }
  def main(args: Array[String]) {
    MySvm()
  }
}
