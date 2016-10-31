package com.edu.mapreduce.map;

import com.edu.mapreduce.dto.CompositeKey;
import com.edu.mapreduce.dto.CompositeValue;
import com.edu.mapreduce.utils.Parser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, CompositeKey, CompositeValue> {

    private CompositeKey compositeKey = new CompositeKey();
    private CompositeValue compositeValue = new CompositeValue();

    private Parser parser = new Parser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        parser.parse(line);

        compositeKey.setiPinyouId(parser.getiPinyouId());
        compositeKey.setTimestamp(parser.getTimestamp());

        compositeValue.setAllLine(line);
        compositeValue.setStreamId(parser.getStreamId());

        context.write(compositeKey, compositeValue);
    }
}
