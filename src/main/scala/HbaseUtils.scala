import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{DoubleWritable, Writable}
import org.apache.hadoop.hbase.util.Writables

object HbaseUtils {

  var conf = HBaseConfiguration.create()
  //conf.set("hbase.zookeeper.quorum", "192.168.213.47:2181,192.168.213.48:2181,192.168.213.49:2181")
  conf.set("hbase.zookeeper.quorum", "qhadoop7,qhadoop8,qhadoop9")
  conf.set("hbase.client.retries.number", "1")
  //conf.set("zookeeper.znode.parent", "/hbase")

  def creatTable(tableName: String, family: Array[String]) {
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val desc: HTableDescriptor = new HTableDescriptor(tableName)
    for (i <- family) {
      desc.addFamily(new HColumnDescriptor(i))
    }
    if (admin.tableExists(tableName)) {
      System.out.println("table Exists!")
      System.exit(0)
    }
    else {
      admin.createTable(desc)
      System.out.println("create table Success!")
    }
  }

  def addData(rowKey: String, tableName: String, familyName: String,
               column: String, value: Writable) {
    val put = new Put(Bytes.toBytes(rowKey))
    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column), Writables.getBytes(value))
    val table: HTable = new HTable(conf, Bytes.toBytes(tableName))
    table.put(put)
  }

  def getResultByColumn(tableName: String, rowKey: String, familyName: String, columnName: String): DoubleArrayWritable = {
    val table: HTable = new HTable(conf, Bytes.toBytes(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName))
    val result: Result = table.get(get)
    val r = new DoubleArrayWritable()
    Writables.getWritable(result.list.get(0).getValue, r)
    table.close()
    r
  }

  def updateTable(tableName: String, rowKey: String, familyName: String, columnName: String, value: DoubleArrayWritable) {
    val table: HTable = new HTable(conf, Bytes.toBytes(tableName))
    val put: Put = new Put(Bytes.toBytes(rowKey))
    put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Writables.getBytes(value))
    table.put(put)
    System.out.println("update table Success!")
    table.close()
  }

  def getResultScan(tableName: String) {
    val scan = new Scan()
    val table = new HTable(conf, Bytes.toBytes(tableName))
    val rs = table.getScanner(scan)
    var r = rs.next()
    while(r != null) {
      val kvs = r.list()
      for (i <- 0 until kvs.size()) {
        println(Bytes.toString(kvs.get(i).getRow()))
        println(Bytes.toString(kvs.get(i).getFamily()))
        println(Bytes.toString(kvs.get(i).getQualifier()))
        println(Bytes.toString(kvs.get(i).getValue()))
        println(kvs.get(i).getTimestamp())
      }
      r = rs.next()
    }
    rs.close()
  }

  def main(args: Array[String]) {
    //creatTable("LR", Array("WEIGHT"))
    val f = Array(new DoubleWritable(1.0), new DoubleWritable(2.0), new DoubleWritable(3.0))
    val t = Array(new DoubleWritable(4.0), new DoubleWritable(5.0), new DoubleWritable(6.0))
    //addData("ROWKEY", "LR", "WEIGHT", "FEATRUE", new DoubleArrayWritable(f))
    //updateTable("LR", "ROWKEY", "WEIGHT", "FEATURE", new DoubleArrayWritable(t))
    val r = getResultByColumn("LR", "ROWKEY", "WEIGHT", "FEATRUE")
    print(r.toArray.asInstanceOf[Array[DoubleWritable]].mkString("\t"))
    //getResultScan("LR")
  }


}