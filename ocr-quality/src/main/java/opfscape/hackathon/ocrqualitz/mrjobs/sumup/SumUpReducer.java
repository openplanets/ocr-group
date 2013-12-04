/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrqualitz.mrjobs.sumup;

import java.io.IOException;
import java.util.Iterator;
import opfscape.hackathon.ocrqualitz.mrjobs.analyse.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author scape
 */
public class SumUpReducer extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntArrayWritable> iterator = values.iterator();
        
        IntWritable[] sum = new IntWritable[4];
        sum[0] = new IntWritable(0);
        sum[1] = new IntWritable(0);
        sum[2] = new IntWritable(0);
        sum[3] = new IntWritable(0);
        
        while (iterator.hasNext()) {
            IntArrayWritable iaw = iterator.next();
            sum[0].set(sum[0].get() + ((IntWritable)iaw.get()[0]).get());
            sum[1].set(sum[1].get() + ((IntWritable)iaw.get()[1]).get());
            sum[2].set(sum[2].get() + ((IntWritable)iaw.get()[2]).get());
            sum[3].set(sum[3].get() + ((IntWritable)iaw.get()[3]).get());
        }
        
        context.write(key, new IntArrayWritable(sum));
    }
    
}
