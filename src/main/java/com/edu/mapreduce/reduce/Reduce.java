package com.edu.mapreduce.reduce;

import com.edu.mapreduce.dto.CompositeKey;
import com.edu.mapreduce.dto.CompositeValue;
import com.edu.mapreduce.utils.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<CompositeKey, CompositeValue, Text, NullWritable> {

    private Max MAX = new Max();

    @Override
    public void reduce(CompositeKey key, Iterable<CompositeValue> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (CompositeValue value : values) {
            context.write(value.getAllLine(), NullWritable.get());
            sum += value.getStreamId().get();
        }

        if (MAX.getMax() < sum) {
            MAX.setMax(sum);
            MAX.setCurrentIPinId(key.getiPinyouId().toString());
        }
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(Constants.COUNTER_GROUP, MAX.getCurrentIPinId()).increment(MAX.getMax());
    }

    private static class Max {
        private String currentIPinId;
        private long max;

        Max() {
            this.currentIPinId = "";
            this.max = 0;
        }

        String getCurrentIPinId() {
            return currentIPinId;
        }

        void setCurrentIPinId(String currentIPinId) {
            this.currentIPinId = currentIPinId;
        }

        long getMax() {
            return max;
        }

        void setMax(long max) {
            this.max = max;
        }
    }
}
