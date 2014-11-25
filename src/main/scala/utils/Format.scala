package utils

import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by zc1 on 14/11/20.
 */
object Format {

  def parseRecFeedBack(lines: DStream[String]) {
    val dataPattern = ("src%3d([a-zA-Z])$action%3d([0-9])$enb%3d([0-9])$csku%3d([0-9,])" +
      "$sku%3d([0-9])$expid%3d([0-9])$rid=([0-9])$sig=([0-9a-z])$ver=([0-9])").r
    lines.map{
      line => {
        val Array(time, recType, ip, uuid, sessionid, pin, source, url, refer, data, md5) = line.split("\t")
        val dataPattern(src, action, enb, csku, sku, expid, rid, sig, ver) = data
        if (action.toInt = 1) {
          (refer+sessionid, (action, csku, sku, recType, uuid))
        } else {
          (url+sessionid, (action, csku.split(",").toSet, sku, recType, uuid))
        }
      }
    }.reduceByKeyAndWindow()

  }

}
