/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrquality.mrjobs.analyse;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author scape
 */
public class AnalyseOCRMapper extends Mapper<LongWritable, Text, FileNameWordKey, IntArrayWritable> {

    private Pattern cleanUpEnd = Pattern.compile(".*[\\s.,\\-)]$");
    private Pattern cleanUpBeg = Pattern.compile("^[$\\s()\\*,\"'.\\-$&].*");
    private static IntWritable one = new IntWritable(1);
    private static IntWritable zero = new IntWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {

            String token = clearToken(tokenizer.nextToken());

            IntWritable[] values = new IntWritable[4];
            boolean isWord = isWord(token);
            // number of correctly recognised words
            values[0] = isWord ? one : zero;
            // number of incorrectly recognised words
            values[1] = !isWord ? one : zero;

            // check with HBase dictionary
            int found = (int) Math.round(Math.random() * 1);

            // number of correctly recognised words
            values[2] = new IntWritable(found);

            // number of incorrectly recognised words
            values[3] = found == 1 ? zero : one;

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            FileNameWordKey fnwk = new FileNameWordKey(filename, token);
            if (isWord) {
                context.write(fnwk, new IntArrayWritable(values));
            }
        }
    }

    private String clearToken(String token) {
//        if (token.length() > 1 && (token.charAt(token.length() - 1) == '.' || token.charAt(token.length() - 1) == ',')) {
//            return token.substring(0, token.length() - 1);
//        }
//        if (token.length() > 1 && (token.charAt(0) == '(')) {
//            return token.substring(1, token.length());
//        }
        boolean cleanEnd = cleanUpEnd.matcher(token).find();
        boolean cleanBeg = cleanUpBeg.matcher(token).find();
        String result = token;

        if (cleanEnd && result.length() > 0) {
            result = result.substring(0, result.length() - 1);
        }
        if (cleanBeg && result.length() > 0) {
            result = result.substring(1, result.length());
        }
        return result;
    }

    private boolean isWord(String token) {
        // check wierd characters in the word
        if (token.length() < 1) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            if (!Character.isLetter(token.charAt(i))) {
                return false;
            }
        }
        return true;

//        boolean isWord = !p.matcher(token).find();
//        return isWord;
    }
}
