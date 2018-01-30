package com.hll

import com.twitter.algebird.{Bytes, DenseHLL, HyperLogLog}
import org.apache.hadoop.hive.ql.exec.{UDAF, UDAFEvaluator}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator

import collection.JavaConverters._


class HyperLogUDAF extends UDAF{

  object HyperLogEvaluator extends UDAFEvaluator {

    class Buffer{
      var count : java.util.List[java.lang.Byte] = _
      var bits:Integer = 0
    }

    var buffer : Buffer = _
    init()

    override  def init(): Unit = {
      buffer = new Buffer
    }

    @throws[HiveException]
    def iterate(input: java.util.List[java.lang.Byte]): Boolean = {
      val in = input.asScala.map(a=> a.asInstanceOf[Byte]).toArray
      val hll = HyperLogLog.fromBytes(in).toDenseHLL
      if (buffer == null) throw new HiveException("Item is not initialized")
      buffer.count= hll.v.array.toList.map(a=> new java.lang.Byte(a)).asJava
      buffer.bits = hll.bits
      true
    }

    def terminatePartial: Buffer = buffer

    def merge(buffer2: Buffer): Boolean = {
      if (buffer.count == null) {
        buffer.count = buffer2.count
        buffer.bits = buffer2.bits
        true
      } else if (buffer.count != null && buffer2.count != null) {
        val state2 = new DenseHLL(buffer2.bits,
          new Bytes(buffer2.count.asScala.map(a=> a.asInstanceOf[Byte]).toArray))
        state2.updateInto(buffer.count.asScala.map(a=> a.asInstanceOf[Byte]).toArray)
        true
      }
      true
    }

    def terminate: java.util.List[java.lang.Byte] = {
      val state = DenseHLL(buffer.bits,new Bytes(buffer.count.asScala.map(a=> a.asInstanceOf[Byte]).toArray))
      HyperLogLog.toBytes(state).toList.map(a=> new java.lang.Byte(a)).asJava
    }
  }
}
