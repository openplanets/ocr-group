/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package opfscape.hackathon.ocrquality.mrjobs.analyse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author scape
 */
public class FileNameWordKey implements WritableComparable<FileNameWordKey> {
    
    private String fileName;
    private String word;

    public FileNameWordKey() {
    }

    public FileNameWordKey(String fileName, String word) {
        this.fileName = fileName;
        this.word = word;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(fileName);
        d.writeUTF(word);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        fileName = di.readUTF();
        word = di.readUTF();
    }

    @Override
    public int compareTo(FileNameWordKey o) {
        int res = fileName.compareTo(o.fileName);
        if (res != 0) {
            return res;
        }
        else {
            return word.compareTo(o.word);
        }
    }

    @Override
    public String toString() {
        return fileName + ", " + word;
    }  
}
