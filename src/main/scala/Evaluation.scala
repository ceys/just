import scala.math._

/**
 * Created by zc1 on 14/11/20.
 */
object Evaluation {

  /**
   * logarithmic loss of p given y
   *
   * @param p our prediction
   * @param y real answer
   * @return
   */
  def logLoss(p: Double, y: Double): Double = {
    val t = max(min(p, 1.0 - 10e-15), 10e-15)
    if (y==1) -log(t) else -log(1.0-t)
  }

}
