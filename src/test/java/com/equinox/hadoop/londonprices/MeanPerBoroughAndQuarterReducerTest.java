package com.equinox.hadoop.londonprices;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MeanPerBoroughAndQuarterReducerTest {

    @Test
    public void should_calculate_mean_prices() throws IOException, InterruptedException {

        MeanPerBoroughAndQuarterReducer reducer = new MeanPerBoroughAndQuarterReducer();

        Reducer.Context context = mock(Reducer.Context.class);
        reducer.reduce(new BoroughQuarterWritable("1995/1", "Barnet"), Arrays.asList(new IntWritable(10), new IntWritable(20)), context);

        verify(context).write(new BoroughQuarterWritable("1995/1", "Barnet"), new IntWritable(15));
    }

}