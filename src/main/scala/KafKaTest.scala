import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._ // 必须import，否则map无法使用
import org.apache.spark.streaming.kafka._
import kafka.serializer.DefaultDecoder

object KafkaTest {
  def main (args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaTest <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaTest")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    println(topicMap)

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

   /* 
    val kafkaParams = Map(
        "zookeeper.connect" -> zkQuorum,
        "group.id" -> group,
        "zookeeper.connection.timeout.ms" -> "1000")
    val lines = KafkaUtils.createStream[String, String, DefaultDecoder, DefaultDecoder](
        ssc,
        kafkaParams,
        topicMap, 
        storageLevel = StorageLevel.MEMORY_ONLY_SER)
    */

    lines.print()

    //val outDir = "yangdekun/kafka/"
    //lines.saveAsTextFiles(outDir, "")

    ssc.start()
    ssc.awaitTermination()
  }
}
