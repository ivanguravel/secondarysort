package com.edu.mapreduce;

import com.edu.mapreduce.dto.CompositeKey;
import com.edu.mapreduce.dto.CompositeValue;
import com.edu.mapreduce.reduce.Reduce;
import com.edu.mapreduce.comparators.GroupingComparator;
import com.edu.mapreduce.comparators.CompositeKeySortComparator;
import com.edu.mapreduce.map.Map;
import com.edu.mapreduce.partitioners.JobPartitioner;
import com.edu.mapreduce.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    private String input = "/user/hadoop/test";
    private String output = "/user/hadoop/test/out";

    public static void main(String... args) throws Exception {
        int exit = ToolRunner.run(new Driver(), args);
        System.exit(exit);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 0 && args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
        }

        if (args.length == 2) {
            input = args[0];
            output = args[1];
        }

        Configuration config = getConf();

        Job job = Job.getInstance(config);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setPartitionerClass(JobPartitioner.class);
        job.setSortComparatorClass(CompositeKeySortComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(CompositeValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(Integer.parseInt(Constants.REDUCERS));

        boolean complete = job.waitForCompletion(true);

        System.out.println("Maximum value for site-impression has record with IPinUId = "
                + findMaximumSiteImpression(job.getCounters()));

        return complete ? 0 : 1;
    }


    private static String findMaximumSiteImpression(final Counters counters) {
        for (CounterGroup counterGroup : counters) {
            if (counterGroup.getName().equals(Constants.COUNTER_GROUP)) {
                for (Counter counter : counterGroup) {
                    if (counter.getValue() != 0) {
                        return counter.getName();
                    }
                }
            }
        }
        return "undefined";
    }
}
