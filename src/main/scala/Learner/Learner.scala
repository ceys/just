package learner

/**
 * Created by zc1 on 14/11/20.
 */
abstract class Learner {

  def predict(x: Array[(Int, Double)]): Double = {}

  def update(x: Array[(Int, Double)], p: Double, y: Int) {}

}
