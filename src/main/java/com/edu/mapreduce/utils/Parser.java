package com.edu.mapreduce.utils;

public class Parser {

    private static final String SYMBOL_FOR_SPLITTING = "\t";

    private String iPinyouId;
    private String timestamp;
    private int streamId;

    public Parser() {
        this.iPinyouId = "";
        this.timestamp = "";
        this.streamId = 0;
    }

    public void parse(String line) {
        String[] afterSplit = line.split(SYMBOL_FOR_SPLITTING);

        this.iPinyouId = afterSplit[1];
        this.timestamp = afterSplit[2];
        this.streamId = getStreamId(line);
    }

    public String getiPinyouId() {
        return iPinyouId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public int getStreamId() {
        return streamId;
    }

    private static int getStreamId(String line) {
        StringBuilder sb = new StringBuilder(line);
        String streamIdInString = sb.reverse().toString().split(SYMBOL_FOR_SPLITTING)[0];
        return Integer.parseInt(streamIdInString);
    }
}
