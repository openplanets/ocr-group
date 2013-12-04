/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrquality.mrjobs.sumup;

import opfscape.hackathon.ocrquality.mrjobs.analyse.IntArrayWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author scape
 */
public class SumUpJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job assessOCR = new Job(getConf(), "assessOCRQuality");
        
        assessOCR.setJarByClass(SumUpJob.class);
        assessOCR.setMapOutputKeyClass(Text.class);
        assessOCR.setMapOutputValueClass(IntArrayWritable.class);
        
        assessOCR.setMapperClass(SumUpMapper.class);
        assessOCR.setCombinerClass(SumUpReducer.class);
        assessOCR.setReducerClass(SumUpReducer.class);
        
        TextInputFormat.addInputPath(assessOCR, new Path(strings[0]));
        TextOutputFormat.setOutputPath(assessOCR, new Path(strings[1]));
        
        assessOCR.setInputFormatClass(TextInputFormat.class);
        assessOCR.setOutputFormatClass(TextOutputFormat.class);
        
        assessOCR.waitForCompletion(true);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SumUpJob(), args);
    }
    
}
