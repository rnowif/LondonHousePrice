package com.equinox.hadoop.londonprices;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class PricesPerBoroughAndQuarterMapperTest {

    public static final String HEAD_LINE = "id,transaction_id,price,date_processed,quarter,month,year,year_month,post_code,property_type,whether_new,tenure,address1,address2,address3,address4,town,local_authority,county,record_status,post_code_clean,inner_outer,borough_code,borough_name,ward_code,ward_name,msoa11,lsoa11,oa11";
    public static final String FIRST_LINE = "861696,{5C1CC576-24F6-453B-9984-3898CC3AA528},155000,11/01/1995 00:00,1,1,1995,1995/1,N20 9AQ,F,Y,L,\"GREENLEAF COURT, 17\",FLAT 1,OAKLEIGH PARK NORTH,LONDON,LONDON,BARNET,GREATER LONDON,A,N20 9AQ,Outer,E09000003,Barnet,E05000058,Oakleigh,E02000031,E01000273,E00001314";

    @Test
    public void should_skip_first_line() throws IOException, InterruptedException {
        PricesPerBoroughAndQuarterMapper mapper = new PricesPerBoroughAndQuarterMapper();
        Mapper.Context context = mock(Mapper.Context.class);

        mapper.map(new LongWritable(1L), new Text(HEAD_LINE), context);

        verifyZeroInteractions(context);
    }

    @Test
    public void should_extract_quarter_borough_price() throws IOException, InterruptedException {
        PricesPerBoroughAndQuarterMapper mapper = new PricesPerBoroughAndQuarterMapper();
        Mapper.Context context = mock(Mapper.Context.class);

        mapper.map(new LongWritable(1L), new Text(FIRST_LINE), context);

        verify(context).write(eq(new BoroughQuarterWritable("1995/1", "Barnet")), eq(new IntWritable(155000)));
    }

}