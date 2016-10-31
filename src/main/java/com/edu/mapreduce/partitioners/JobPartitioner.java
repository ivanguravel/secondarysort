package com.edu.mapreduce.partitioners;

import com.edu.mapreduce.dto.CompositeKey;
import com.edu.mapreduce.dto.CompositeValue;
import org.apache.hadoop.mapreduce.Partitioner;

public class JobPartitioner extends Partitioner<CompositeKey, CompositeValue> {

    @Override
    public int getPartition(CompositeKey compositeKey, CompositeValue value, int numPartitions) {
        return (compositeKey.getiPinyouId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
