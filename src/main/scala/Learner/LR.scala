package learner

import scala.math._

/**
 * Created by zc1 on 14/11/20.
 */
class LR(D: Int) extends Learner {

  val w = Array(D)

  def predict(x: Array[(Int, Double)]): Double = {
    var wTw = 0.0
    for ((i, value) <- x) {
      wTw += w(i) * value
    }
    1 / (1 + exp(-y * wTw))
  }

  def update(x: Array[(Int, Double)], p: Double, y: Int) {
    val scale = (p - 1) * y
    for ((i, value) <- x) {
      w(i) -= value * scale
    }
  }

}
