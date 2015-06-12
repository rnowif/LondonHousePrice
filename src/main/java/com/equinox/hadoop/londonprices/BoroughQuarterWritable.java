package com.equinox.hadoop.londonprices;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BoroughQuarterWritable implements WritableComparable<BoroughQuarterWritable> {

    private String quarter;
    private String borough;

    public BoroughQuarterWritable(String quarter, String borough) {
        this.quarter = quarter;
        this.borough = borough;
    }

    public BoroughQuarterWritable() {
    }

    @Override
    public int compareTo(BoroughQuarterWritable o) {
        if (!quarter.equals(o.quarter))
            return quarter.compareTo(o.quarter) < 0 ? -1 : 1;

        if (!borough.equals(o.borough))
            return borough.compareTo(o.borough) < 0 ? -1 : 1;

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(quarter);
        dataOutput.writeUTF(borough);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        quarter = dataInput.readUTF();
        borough = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BoroughQuarterWritable that = (BoroughQuarterWritable) o;

        if (quarter != null ? !quarter.equals(that.quarter) : that.quarter != null) return false;
        return !(borough != null ? !borough.equals(that.borough) : that.borough != null);

    }

    @Override
    public int hashCode() {
        int result = quarter != null ? quarter.hashCode() : 0;
        result = 31 * result + (borough != null ? borough.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return quarter + "\t" + borough;
    }
}
