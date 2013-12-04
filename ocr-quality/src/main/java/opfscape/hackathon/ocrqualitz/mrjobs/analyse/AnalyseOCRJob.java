/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrqualitz.mrjobs.analyse;

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
public class AnalyseOCRJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new AnalyseOCRJob(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        
        Job assessOCR = new Job(getConf(), "assessOCRQuality");
        
        assessOCR.setJarByClass(AnalyseOCRJob.class);
        assessOCR.setMapOutputKeyClass(FileNameWordKey.class);
        assessOCR.setMapOutputValueClass(IntArrayWritable.class);
        
        assessOCR.setMapperClass(AnalyseOCRMapper.class);
        assessOCR.setCombinerClass(AnalyseOCRReducer.class);
        assessOCR.setReducerClass(AnalyseOCRReducer.class);
        
        TextInputFormat.addInputPath(assessOCR, new Path(strings[0]));
        TextOutputFormat.setOutputPath(assessOCR, new Path(strings[1]));
        
        assessOCR.setInputFormatClass(TextInputFormat.class);
        assessOCR.setOutputFormatClass(TextOutputFormat.class);
        
        assessOCR.waitForCompletion(true);
        
        return 0;
    }
}
