import breeze.linalg.{DenseVector, Vector}
import org.apache.hadoop.hbase.client.{Put, Get, HTablePool}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.{Writables, Bytes}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Random


/**
 * Created by zc1 on 14/11/3.
 */
object FTRL {

  val D = 10   // Number of dimensions
  val R = 0.7  // Scaling factor
  val rand = new Random(42)

  val lambda1 = 0.01
  val lambda2 = 0.01
  val alpha = 0.01
  val beta = 1.0

  case class Sample(x: Array[(Int, Double)], y: Double)

  def updateWeight(sample: Sample, w: Array[Double], z: Array[Double], n: Array[Double]) {
    var p = 0.0
    for (xi <- sample.x) {
      val i = xi._1
      if (math.abs(z(i)) <= lambda1) {
        w(i) = 0
      } else {
        w(i) = (sgn(z(i)) * lambda1 - z(i)) / ( (beta + math.pow(n(i), 0.5)) / alpha + lambda2)
      }
      p += w(i) * xi._2
    }
    p = sigmoid(p)
    for (xi <- sample.x) {
      val i = xi._1
      val gi = (p - sample.y) * xi._2
      val sigma = (math.pow(n(i) + gi * gi, 0.5) - math.pow(n(i), 0.5)) / alpha
      z(i) += gi - sigma * w(i)
      n(i) += gi * gi
    }
  }

  def sigmoid(x: Double): Double = {
    1 / (1 + math.exp(-x))
  }

  def sgn(x: Double): Int = {
    if (x>0) 1
    else -1
  }


  def generateSample(i: Int) = {
    def generateFeature() = {(rand.nextInt(D), rand.nextGaussian())}
    val y = if(i % 2 == 0) -1 else 1
    val x = Array.fill(rand.nextInt(D))(generateFeature)
    Sample(x, y)
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

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2.hashCode)

    val conf = HBaseConfiguration.create()
    //conf.set("hbase.zookeeper.quorum", "192.168.213.47:2181,192.168.213.48:2181,192.168.213.49:2181")
    conf.set("hbase.zookeeper.quorum", "qhadoop7,qhadoop8,qhadoop9")
    conf.set("hbase.client.retries.number", "1")
    val htablePool = new HTablePool(conf, 100);

    lines.map(generateSample).foreachRDD( rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val table = htablePool.getTable("LR");
        val getZ: Get = new Get(Bytes.toBytes("ROWKEY")).addColumn(Bytes.toBytes("FAMILYNAME"), Bytes.toBytes("Z"))
        val getN: Get = new Get(Bytes.toBytes("ROWKEY")).addColumn(Bytes.toBytes("FAMILYNAME"), Bytes.toBytes("N"))
        val putZ: Put = new Put(Bytes.toBytes("ROWKEY"))
        val putN: Put = new Put(Bytes.toBytes("ROWKEY"))
        val zw = new DoubleArrayWritable()
        val nw = new DoubleArrayWritable()
        val w = Array.fill(D)(0.0)
        partitionOfRecords.foreach(p => {
          Writables.getWritable(table.get(getZ).list.get(0).getValue, zw)
          Writables.getWritable(table.get(getN).list.get(0).getValue, nw)
          val z = zw.toArray.asInstanceOf[Array[Double]]
          val n = nw.toArray.asInstanceOf[Array[Double]]

          updateWeight(p, w, z, n)

          zw.fromArray(z)
          nw.fromArray(n)
          putZ.add(Bytes.toBytes("FAMILYNAME"), Bytes.toBytes("Z"), Writables.getBytes(zw))
          putN.add(Bytes.toBytes("FAMILYNAME"), Bytes.toBytes("N"), Writables.getBytes(nw))
          table.put(putZ)
          table.put(putN)
        })
      })
    })

  }

}
