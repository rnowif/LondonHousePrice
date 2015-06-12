package com.equinox.hadoop.londonprices;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PricesPerBoroughAndQuarterMapper extends Mapper<LongWritable, Text, BoroughQuarterWritable, IntWritable> {

    private static final int PRICE_INDEX = 2;
    private static final int QUARTER_INDEX = 4;
    private static final int YEAR_INDEX = 6;
    private static final int BOROUGH_INDEX = 23;
    public static final String COMMA_SEPARATED_SPLIT_REGEX = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(COMMA_SEPARATED_SPLIT_REGEX);

        if (headline(fields)) {
            return;
        }

        int price = Integer.valueOf(fields[PRICE_INDEX]);
        String quarter = fields[QUARTER_INDEX];
        String year = fields[YEAR_INDEX];
        String borough = fields[BOROUGH_INDEX];

        context.write(new BoroughQuarterWritable(buildQuarter(quarter, year), borough), new IntWritable(price));
    }

    private String buildQuarter(String quarter, String year) {
        return year + "/" + quarter;
    }

    private boolean headline(String[] fields) {
        return fields[0].equalsIgnoreCase("id");
    }
}
