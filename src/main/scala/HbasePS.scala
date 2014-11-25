import org.apache.hadoop.hbase.client.{Put, Get, HTable}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.{Writables, Bytes}

/**
 *
 * @param hbaseZK
 * @param tableName
 */
class HbasePS(hbaseZK: String, tableName: String, familyName: String) {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", hbaseZK)
  conf.set("hbase.client.retries.number", "1")
  val table = new HTable(conf, tableName)

  def loadData()


  def pull(rowKey: String, columnName: String, weight: DoubleArrayWritable) = {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(familyName)
    val result = table.get(get)
    Writables.getWritable(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)), weight)
    weight.toArray.asInstanceOf[Array[Double]]
  }


  def push(rowKey: String, columnNames: Array[String], weights: Array[DoubleArrayWritable]) {
    val put: Put = new Put(Bytes.toBytes(rowKey))
    for (i <- 0 until weights.length) {
      put.add(familyName, Bytes.toBytes(columnNames(i)), Writables.getBytes(weights(i)))
    }
    table.put(put)
  }

}
