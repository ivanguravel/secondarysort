package com.edu.mapreduce;

import com.edu.mapreduce.dto.CompositeKey;
import com.edu.mapreduce.dto.CompositeValue;
import com.edu.mapreduce.reduce.Reduce;
import com.edu.mapreduce.utils.Constants;
import com.edu.mapreduce.map.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;



public class DriverTest {


    private static final String line1 = "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t1";
    private static final String line2 = "7b6195de0d14203f92001da653bf1de\t20130606000104000\tVhkr1vpROHuhQWB\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0),gzip(gfe),gzip(gfe)\t113.119.105.*\t216\t217\t2\ttrqRTuToMTNUjM9r5rMi\t74419a072f8927222a1fd8aaa18cce56\t\t433287550\t468\t60\t1\t0\t5\t2f88fc9cf0141b5bbaf251cab07f4ce7\t300\t3386\t282825712733\t0";
    private static final String line3 = "2ea9fe21cf7350fcb5696d8cff0bbeaa\t20130606000104000\tVhKdLnuY3tlhXMa\tmozilla/4.0 (compatible; msie 8.0; windows nt 5.1; trident/4.0; .net clr 2.0.50727)\t42.184.148.*\t65\t69\t1\ttrqRTvpogNlyDok4JKTI\t134db65c2b66d8468d00bf42fd9f912f\t\tmm_10032051_2374052_9219530\t950\t90\t1\t1\t0\t23d6dade7ed21cea308205b37594003e\t227\t3427\t282163091140\t0";

    @Test
    public void mapTest() throws IOException {
        MapDriver<LongWritable, Text, CompositeKey, CompositeValue> driver = new MapDriver<>();
        driver.getConfiguration().set("mapreduce.job.reduces", Constants.REDUCERS);
        driver.withMapper(new Map())
                .withInput(new LongWritable(0), new Text(line1))
                .withInput(new LongWritable(0), new Text(line2))
                .withInput(new LongWritable(0), new Text(line3))
                .withOutput(new CompositeKey("20130606000104000", "Vh16OwT6OQNUXbj"), new CompositeValue(line1, 1))
                .withOutput(new CompositeKey("20130606000104000", "Vhkr1vpROHuhQWB"), new CompositeValue(line2, 0))
                .withOutput(new CompositeKey("20130606000104000", "VhKdLnuY3tlhXMa"), new CompositeValue(line3, 0))
                .runTest();
    }

    @Test
    public void reduceTest() throws IOException {
        ReduceDriver<CompositeKey, CompositeValue, Text, NullWritable> driver = new ReduceDriver<>();
        driver.getConfiguration().set("mapreduce.job.reduces", Constants.REDUCERS);
        driver.withReducer(new Reduce())
                .withInput(new CompositeKey("20130606000104000", "Vh16OwT6OQNUXbj"), Collections.singletonList(new CompositeValue(line1, 1)))
                .withInput(new CompositeKey("20130606000104000", "Vhkr1vpROHuhQWB"), Collections.singletonList(new CompositeValue(line2, 0)))
                .withOutput(new Text(line1), NullWritable.get())
                .withOutput(new Text(line2), NullWritable.get())
                .runTest();
    }

    @Test
    public void localTest() throws Exception {
        Configuration config = new Configuration();

        config.addResource("config/ConfigurationLocal.xml");

        String input = "input/";
        String output = "output/";

        FileSystem fs = FileSystem.getLocal(config);

        fs.delete(new Path(output), true);

        Driver driver = new Driver();

        driver.setConf(config);
        int exit = driver.run(new String[]{input, output});

        assertEquals(0, exit);
    }
}
