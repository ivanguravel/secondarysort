package com.edu.mapreduce.dto;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements Writable, WritableComparable<CompositeKey> {

    private Text iPinyouId;
    private Text timestamp;

    @SuppressWarnings("unused")
    public CompositeKey() {
        this.iPinyouId = new Text("");
        this.timestamp = new Text("");

    }

    public CompositeKey(String iPinyouId, String timestamp) {
        this.iPinyouId = new Text(iPinyouId);
        this.timestamp = new Text(timestamp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        iPinyouId.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        iPinyouId.readFields(in);
        timestamp.readFields(in);

    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = this.iPinyouId.compareTo(o.iPinyouId);
        return result == 0 ? this.timestamp.compareTo(o.timestamp) : result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeKey)) return false;

        CompositeKey that = (CompositeKey) o;

        if (iPinyouId != null ? !iPinyouId.equals(that.iPinyouId) : that.iPinyouId != null) return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;
    }

    public Text getiPinyouId() {
        return iPinyouId;
    }

    public void setiPinyouId(String iPinyouId) {
        this.iPinyouId.set(iPinyouId);
    }

    public void setTimestamp(String timestamp) {
        this.timestamp.set(timestamp);
    }
}
