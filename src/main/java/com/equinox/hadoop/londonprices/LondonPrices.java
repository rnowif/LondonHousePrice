package com.equinox.hadoop.londonprices;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LondonPrices extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LondonPrices(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Mean per quarter and borough");
        job.setJarByClass(getClass());

        job.setMapperClass(PricesPerBoroughAndQuarterMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        job.setMapOutputKeyClass(BoroughQuarterWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MeanPerBoroughAndQuarterReducer.class);
        job.setOutputKeyClass(BoroughQuarterWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
