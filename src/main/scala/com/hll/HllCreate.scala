package com.hll

import com.twitter.algebird.{HyperLogLog, HyperLogLogMonoid}
import org.apache.hadoop.hive.ql.exec.UDF
import collection.JavaConverters._


class HllCreate extends UDF {
  def evaluate(x: String, bits: Integer): java.util.List[java.lang.Byte] = {
    val monoid = new HyperLogLogMonoid(bits.toInt)
    HyperLogLog.toBytes(monoid.toHLL(x)).toList.map(a=> new java.lang.Byte(a)).asJava
  }
}