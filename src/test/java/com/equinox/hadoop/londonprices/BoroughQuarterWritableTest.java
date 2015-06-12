package com.equinox.hadoop.londonprices;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.DataOutput;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BoroughQuarterWritableTest {

    @Test
    public void should_read_from_input() throws IOException {
        BoroughQuarterWritable writable = new BoroughQuarterWritable();

        writable.readFields(new StringsDataInput("1995/1", "Barnet"));

        assertThat(writable, is(new BoroughQuarterWritable("1995/1", "Barnet")));
    }


    private class StringsDataInput extends AbstractDataInput {
        private final String[] fields;
        private int currentPosition = 0;

        public StringsDataInput(String... fields) {
            this.fields = fields;
        }

        @Override
        public String readUTF() throws IOException {
            return fields[currentPosition++];
        }
    }

    @Test
    public void should_write_to_output() throws IOException {
        BoroughQuarterWritable writable = new BoroughQuarterWritable("1995/1", "Barnet");

        DataOutput dataOutput = mock(DataOutput.class);
        writable.write(dataOutput);

        verify(dataOutput).writeUTF("1995/1");
        verify(dataOutput).writeUTF("Barnet");
    }

    @Test
    public void should_be_equal_if_both_fields_are_equals() {
        BoroughQuarterWritable writable = new BoroughQuarterWritable("1995/1", "Barnet");
        BoroughQuarterWritable otherWritable = new BoroughQuarterWritable("1995/1", "Barnet");

        assertThat(writable.compareTo(otherWritable), is(0));
    }

    @Test
    public void should_be_inferior_if_quarter_is_inferior() {
        BoroughQuarterWritable writable = new BoroughQuarterWritable("1994/1", "Barnet");
        BoroughQuarterWritable otherWritable = new BoroughQuarterWritable("1996/1", "Barnet");

        assertThat(writable.compareTo(otherWritable), is(-1));
    }

    @Test
    public void should_be_inferior_if_same_quarter_and_borough_is_inferior() {
        BoroughQuarterWritable writable = new BoroughQuarterWritable("1994/1", "Aarnet");
        BoroughQuarterWritable otherWritable = new BoroughQuarterWritable("1994/1", "Barnet");

        assertThat(writable.compareTo(otherWritable), is(-1));
    }

    @Test
    public void should_be_superior_if_quarter_is_superior() {
        BoroughQuarterWritable writable = new BoroughQuarterWritable("1996/1", "Barnet");
        BoroughQuarterWritable otherWritable = new BoroughQuarterWritable("1994/1", "Barnet");

        assertThat(writable.compareTo(otherWritable), is(1));
    }

    @Test
    public void should_be_superior_if_same_quarter_and_borough_is_superior() {
        BoroughQuarterWritable writable = new BoroughQuarterWritable("1994/1", "Barnet");
        BoroughQuarterWritable otherWritable = new BoroughQuarterWritable("1994/1", "Aarnet");

        assertThat(writable.compareTo(otherWritable), is(1));
    }
}