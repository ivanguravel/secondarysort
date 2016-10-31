package com.edu.mapreduce.dto;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValue implements Writable {

    private Text allLine;
    private IntWritable streamId;

    @SuppressWarnings("unused")
    public CompositeValue() {
        this.allLine = new Text("");
        this.streamId = new IntWritable(0);
    }

    public CompositeValue(String allLine, int streamId) {
        this.allLine = new Text(allLine);
        this.streamId = new IntWritable(streamId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        allLine.write(out);
        streamId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        allLine.readFields(in);
        streamId.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeValue)) return false;

        CompositeValue that = (CompositeValue) o;

        if (allLine != null ? !allLine.equals(that.allLine) : that.allLine != null) return false;
        return streamId != null ? streamId.equals(that.streamId) : that.streamId == null;
    }

    @Override
    public String toString() {
        return "CompositeValue{" +
                "allLine=" + allLine +
                ", streamId=" + streamId +
                '}';
    }

    public Text getAllLine() {
        return allLine;
    }

    public IntWritable getStreamId() {
        return streamId;
    }

    public void setAllLine(String allLine) {
        this.allLine.set(allLine);
    }

    public void setStreamId(int streamId) {
        this.streamId.set(streamId);
    }
}
