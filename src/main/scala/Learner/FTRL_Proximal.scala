package learner

import scala.math._
import org.apache.spark.mllib.linalg.SparseVector

/**
 * Follow the regularized leader-proximal
 * Reference: "Ad click Prediction: A View from the Trenches"
 *
 * @param alpha depend on the features and dataset
 * @param beta beta=1 is usually good enough
 * @param L1
 * @param L2
 * @param D feature dimension
 * @param interaction interaction feature
 */
class FTRL_Proximal(alpha: Double, beta: Double, L1: Double, L2: Double,
                    D: Int, interaction: Boolean)
  extends Learner {

  var z = Array(D)
  var n = Array(D)
  var w = Array(D)

  /**
   * Get probability estimation on x
   *
   * @param x features
   * @return probability of p(y=1|x;w)
   */
  def predict(x: Array[(Int, Double)]): Double = {
    var wTx = 0.0
    for ((i, value) <- x) {
      val sign = if (z(i) < 0) 1 else -1

      if (sign * z(i) <= L1) w(i) = 0
      else w(i)  = (sign * L1 - z(i)) / (beta + sqrt(n(i)) / alpha + L2)
      wTx += w(i)
    }
    // bounded sigmoid function
    1.0 / (1.0 + exp(max((min(wTx, 35.0), -35.0))))
  }

  /**
   * Update z and n using x,p,y
   *
   * @param x features, a list of (index, value)
   * @param p click probability prediction of our model
   * @param y answer
   */
  def update(x: Array[(Int, Double)], p: Double, y: Int) {
    val g = p - y
    for ((i, value) <- x) {
      val sigma = (sqrt(n(i) + g*g) - sqrt(n(i))) / alpha
      z(i) += g - sigma * w(i)
      n(i) += g * g
    }
  }

}

