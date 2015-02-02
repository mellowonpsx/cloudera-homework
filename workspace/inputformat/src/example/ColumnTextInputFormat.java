package example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class ColumnTextInputFormat extends FileInputFormat<Text,Text> {


	private int recordwidth = 50;
	
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		
		RecordReader<Text,Text> recordreader = (RecordReader<Text, Text>) new ColumnTextRecordReader();
		recordreader.initialize(split, context);
		return recordreader;
	    
	}
	
	@Override
    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
        long defaultSize = super.computeSplitSize(blockSize, minSize, maxSize);
		
        // 1st, if the default size is less than the length of a
        // raw record, lets bump it up to a minimum of at least ONE record length
        if (defaultSize < recordwidth)
        	return recordwidth;
 
        // determine the split size, it should be as close as possible to the
        // default size, but should NOT split within a record... each split
        // should contain a complete set of records with the first record
        // starting at the first byte in the split and the last record ending
        // with the last byte in the split.
 
        long splitSize = ((long)(Math.floor((double)defaultSize / (double)recordwidth))) * recordwidth;
 
        return splitSize;
 
    }

}
