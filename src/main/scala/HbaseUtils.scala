import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.util.Bytes

object HbaseUtils {

  var conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "192.168.213.47:2181,192.168.213.48:2181,192.168.213.49:2181")
  conf.set("hbase.client.retries.number", "1")
  conf.set("zookeeper.znode.parent", "/hbase")

  val htablePool = new HTablePool();

  def creatTable(tableName: String, family: Array[String]) {
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val desc: HTableDescriptor = new HTableDescriptor(tableName)
    {
      var i: Int = 0
      while (i < family.length) {
        {
          desc.addFamily(new HColumnDescriptor(family(i)))
        }
        ({
          i += 1; i - 1
        })
      }
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

  def getResultByColumn(tableName: String, rowKey: String, familyName: String, columnName: String): String = {
    val table: HTable = new HTable(conf, Bytes.toBytes(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName))
    val result: Result = table.get(get)
    return Bytes.toString(result.list.get(0).getValue)
  }

  def updateTable(tableName: String, rowKey: String, familyName: String, columnName: String, value: String) {
    val table: HTable = new HTable(conf, Bytes.toBytes(tableName))
    val put: Put = new Put(Bytes.toBytes(rowKey))
    put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value))
    table.put(put)
    System.out.println("update table Success!")
  }



}