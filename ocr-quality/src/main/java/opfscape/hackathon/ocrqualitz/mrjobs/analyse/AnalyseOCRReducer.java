/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrqualitz.mrjobs.analyse;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author scape
 */
public class AnalyseOCRReducer extends Reducer<FileNameWordKey, IntArrayWritable, FileNameWordKey, IntArrayWritable> {

    @Override
    protected void reduce(FileNameWordKey key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        int sum[] = new int[4];
        Iterator<IntArrayWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntArrayWritable arr = iterator.next();
            
            sum[0] += ((IntWritable)arr.get()[0]).get();
            sum[1] += ((IntWritable)arr.get()[1]).get();
            sum[2] += ((IntWritable)arr.get()[2]).get();
            sum[3] += ((IntWritable)arr.get()[3]).get();
        }
        
        IntWritable[] output = new IntWritable[4];
        output[0] = new IntWritable(sum[0]);
        output[1] = new IntWritable(sum[1]);
        output[2] = new IntWritable(sum[2]);
        output[3] = new IntWritable(sum[3]);
        
        context.write(key, new IntArrayWritable(output));
    }
    
}
