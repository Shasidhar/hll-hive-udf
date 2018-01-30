package com.hll;

import com.twitter.algebird.Bytes;
import com.twitter.algebird.DenseHLL;
import com.twitter.algebird.HyperLogLog;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.convert.Decorators;

import java.util.ArrayList;
import java.util.List;

public class HLLUdaf extends UDAF{
    public static class HLLUdafEvaluator implements UDAFEvaluator {

        public static class Buffer {
            ArrayList<Byte> count = null;
            int bits = 0;
        }

        private Buffer buffer = null;

        public HLLUdafEvaluator() {
            super();
            init();
        }

        public void init() {
            buffer = new Buffer();
        }

        public boolean iterate(ArrayList<Byte> value) throws HiveException {
            if (buffer == null) throw new HiveException("Item is not initialized");
            DenseHLL denseHLL = HyperLogLog.fromBytes(convertToArray(value)).toDenseHLL();
            buffer.count = convertToList(denseHLL.v().array());
            buffer.bits = denseHLL.bits();
            return true;
        }

        public Buffer terminatePartial() {
            return buffer;
        }

        public boolean merge(Buffer other) {
            if(buffer.count == null) {
                buffer.count = other.count;
                buffer.bits = other.bits;
            } else if(other.count != null) {
                DenseHLL denseHLL = new DenseHLL(other.bits,
                        new Bytes(convertToArray(other.count)));
                denseHLL.updateInto(convertToArray(other.count));
            }
            return true;
        }

        private byte[] convertToArray(ArrayList<Byte> list) {
            byte[] returnArray = new byte[list.size()];
            for(int i=0;i<list.size();i++){
                returnArray[i] = list.get(i);
            }
            return returnArray;
        }

        private ArrayList<Byte> convertToList(byte[] data) {
            ArrayList<Byte> result = new ArrayList<Byte>();
            for (byte anAbc : data) {
                result.add(anAbc);
            }
            return result;
        }

        public List<Byte> terminate() {
            DenseHLL denseHLL = new DenseHLL(buffer.bits,new Bytes(convertToArray(buffer.count)));
            return convertToList(HyperLogLog.toBytes(denseHLL));
        }

    }
}
