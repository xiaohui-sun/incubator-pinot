package com.linkedin.pinot.core.segment.index.readers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;

public class BloomFilterReader {
  BloomFilter _bloomFilter;
  
  public BloomFilterReader(PinotDataBuffer bloomFilterBuffer, DataType dataType) throws IOException{
    byte[] buffer = new byte[(int)bloomFilterBuffer.size()];
    bloomFilterBuffer.copyTo(0, buffer);
    _bloomFilter = BloomFilter.deserialize(buffer);
  }
  
  public boolean mightContain(Object key){
    return _bloomFilter.isPresent(key.toString());
  }
  
  public static void main(String[] args) throws Exception {
    FileInputStream fileInputStream = new FileInputStream(new File("/home/kgopalak/pinot_perf/index_dir/tpch_lineitem_OFFLINE/tpch_lineitem_0/l_orderkey.bloom.inv"));
    byte[] buffer = IOUtils.toByteArray(fileInputStream);
    BloomFilter bloomFilter = BloomFilter.deserialize(buffer);
    System.out.println(bloomFilter.buckets());
  }
}
