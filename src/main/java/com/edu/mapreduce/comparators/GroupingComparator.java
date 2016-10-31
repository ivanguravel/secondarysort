package com.edu.mapreduce.comparators;

import com.edu.mapreduce.dto.CompositeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

    protected GroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        CompositeKey key1 = (CompositeKey) o1;
        CompositeKey key2 = (CompositeKey) o2;

        return key1.getiPinyouId().compareTo(key2.getiPinyouId());
    }
}
