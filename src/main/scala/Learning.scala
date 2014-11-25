import learner.FTRL_Proximal
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by zc1 on 14/11/20.
 */
object Learning {

  def learnOnStream(lines: DStream[LabeledPoint]) {
    lines.foreachRDD( rdd => {
      rdd.foreachPartition(partitionOfRecords => {

        val learner = new FTRL_Proximal()
        var p = 0.0
        var count = 0
        var loss = 0.0

        val hbasePs = new HbasePS(hbaseZK, tableName)
        // Array( z, n)
        val weights = Array(new DoubleArrayWritable(), new DoubleArrayWritable())

        partitionOfRecords.zipWithIndex.foreach( (point: LabeledPoint, index: Int) => {

          if (!local && index % nFetch = 0) {
            learner.z = hbasePs.pull(rowkey, ZcolumnName, weights(0))
            learner.n = hbasePs.pull(rowkey, NcolumnName, weights(1))
          }

          p = learner.predict(point.features)
          if (index % holdout == 0) {
            loss += Evaluation.logLoss(p, point.label)
            count += 1
            println("examples: $index, avg logLoss: ${loss / count}")
          }
          learner.update(point.features, p, point.label)

          if (!local && index % nPush = 0) {
            weights(0).fromArray(learner.z)
            weights(1).fromArray(learner.n)
            hbasePs.push(rowkey, Array(ZcolumnName, NcolumnName), weights)
          }

        })
      })
    })
  }


  def main(args: Array[String]) {

    val Array(kafkaZK, group, topics, numThreads, hbaseZK, interval, name) = args

    val sparkConf = new SparkConf().setAppName(name).setMaster("local")// test
    val ssc =  new StreamingContext(sparkConf, Seconds(interval.toInt))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, kafkaZK, group, topicMap).map()

    learnOnStream(lines)
  }

}
