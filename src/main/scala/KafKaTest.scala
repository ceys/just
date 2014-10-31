import org.apache.hadoop.hbase.client.{Get, HTableInterface, HTablePool}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import scala.util.Random
import org.apache.spark.streaming.kafka._
import breeze.linalg.{Vector, DenseVector}


object KafkaTest {

  val D = 10   // Number of dimensions
  val R = 0.7  // Scaling factor
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def generatePoint(i: Int) = {
    val y = if(i % 2 == 0) -1 else 1
    val x = DenseVector.fill(D){rand.nextGaussian + y * R}
    DataPoint(x, y)
  }

  def getHbase() {
    HbaseUtils.getResultByColumn("LR", "ROW", "F", "C")
  }

  def updateHbase(w: DenseVector) {
    HbaseUtils.updateTable("LR", "ROW", "F", "C")
  }


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

    var w = DenseVector.fill(D){2 * rand.nextDouble - 1}

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val htablePool = new HTablePool();

    lines.map(generatePoint).foreachRDD( rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val table = htablePool.getTable("LR");
        val get: Get = new Get(Bytes.toBytes("ROWKEY")).addColumn(Bytes.toBytes("FAMILYNAME"), Bytes.toBytes("COLUMNAME"))
        val w = (table.get(get).list.get(0).getValue)
        partitionOfRecords.foreach(p => {
          val scale = (1 / (1 + math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
          w -= p.x * scale
        })
      })
    })


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
