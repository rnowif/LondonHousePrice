package com.equinox.hadoop.londonprices;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanPerBoroughAndQuarterReducer extends Reducer<BoroughQuarterWritable, IntWritable, BoroughQuarterWritable, IntWritable> {

    @Override
    protected void reduce(BoroughQuarterWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int nbValues = 0;

        for (IntWritable price : values) {
            sum += price.get();
            nbValues++;
        }

        int mean = nbValues == 0 ? 0 : sum / nbValues;
        context.write(key, new IntWritable(mean));
    }
}
