package com.hll

import com.twitter.algebird.HyperLogLog
import org.apache.hadoop.hive.ql.exec.UDF
import collection.JavaConverters._

class HllCardinality extends UDF {
  def evaluate(hll: java.util.List[java.lang.Byte]): Long = {
    val data = hll.asScala.map(a=> a.asInstanceOf[Byte]).toArray
    HyperLogLog.fromBytes(data).approximateSize.estimate
  }
}