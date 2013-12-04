/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrqualitz.mrjobs.sumup;

import java.io.IOException;
import java.util.StringTokenizer;
import opfscape.hackathon.ocrqualitz.mrjobs.analyse.IntArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author scape
 */
public class SumUpMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, " ,");
        
        
        String fileName = tokenizer.nextToken().trim();
        // ommit word
        tokenizer.nextToken().trim();
        
        
        String isWord = tokenizer.nextToken().trim();
        
        while (!isNumber(isWord)) {
            isWord = tokenizer.nextToken().trim();
        }
        
        String notAword = tokenizer.nextToken().trim();
        String inDictionary = tokenizer.nextToken().trim();
        String notInDictionary = tokenizer.nextToken().trim();
        IntWritable[] arr = new IntWritable[4];
        arr[0] = new IntWritable(Integer.valueOf(isWord));
        arr[1] = new IntWritable(Integer.valueOf(notAword));
        arr[2] = new IntWritable(Integer.valueOf(inDictionary));
        arr[3] = new IntWritable(Integer.valueOf(notInDictionary));
        context.write(new Text(fileName), new IntArrayWritable(arr));
    }

    private boolean isNumber(String word) {
        if (word.length() == 0) {
            return false;
        }
        for (int i = 0; i<word.length(); i++) {
            if (!Character.isDigit(word.charAt(i))) {
                return false;
            }
        }
        return true;
    }
    
}
