/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrquality.mrjobs.analyse;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author scape
 */
public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }
    
    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public String[] toStrings() {
        return getStrings();
    }

    private String[] getStrings() {
        String[] result = new String[4];
        Writable[] values = get();
        result[0] = String.valueOf(((IntWritable)values[0]).get());
        result[1] = String.valueOf(((IntWritable)values[1]).get());
        result[2] = String.valueOf(((IntWritable)values[2]).get());
        result[3] = String.valueOf(((IntWritable)values[3]).get());
        return result;
    }

    @Override
    public String toString() {
        String[] result = getStrings();
        return "," + StringUtils.arrayToString(result);
    }
}
