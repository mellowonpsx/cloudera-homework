package example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ColumnTextRecordReader extends RecordReader<Text,Text> {

	private Text key = null;
	private Text value = null;
	
	private long start;				// byte offset into the file for this split
	private long pos;				// current location in the file
	private long end;				// end offset in the file for this split
	
	private FSDataInputStream fileIn; // input stream for the file
	
	private int keywidth=7; 		// number of bytes of the key field
	private int lastnamewidth=25; 	// number of bytes of the surname field
	private int firstnamewidth=10; 	// number of bytes of the first name field
	private int datewidth=8; 		// number of bytes of the date field
	
	byte[] keybytes = new byte[keywidth];
	byte[] datebytes = new byte[datewidth];
	byte[] lastnamebytes = new byte[lastnamewidth];
	byte[] firstnamebytes = new byte[firstnamewidth];
	
	@Override
	public boolean nextKeyValue() throws IOException {

	    if (pos >= end) 
	    	return false;
	    
	    try {
	    	fileIn.readFully(pos,keybytes);
		    pos = pos + keywidth;
		    fileIn.readFully(pos,lastnamebytes);
		    pos = pos + lastnamewidth;
		    fileIn.readFully(pos,firstnamebytes);
		    pos = pos + firstnamewidth;
		    fileIn.readFully(pos,datebytes);
		    pos = pos + datewidth;
		    
	    } catch(IOException e) {
	    	key = null;
	    	value = null;
	    	return false;
	    }
	    
	    key = new Text(keybytes);
	    String valuestring = new String(lastnamebytes).trim() + "," + new String(firstnamebytes).trim() + "\t" + new String(datebytes).trim();
	    value = new Text(valuestring);

	    return true;
	}

	@Override
	public Text getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		// get the file name and start and end positions for this split
		FileSplit split = (FileSplit) genericSplit;
	    this.start = split.getStart();
	    this.end = start + split.getLength();
	    this.pos = start;
	    Path file = split.getPath();	
	    
	    // open the file and seek to the start of the split
	    Configuration job = context.getConfiguration();
	    FileSystem fs = file.getFileSystem(job);
	    fileIn = fs.open(file);
	    
	    // TODO: allow configuration of field widths
	    
	    // create byte buffers to hold the input
		keybytes = new byte[keywidth];
		datebytes = new byte[datewidth];
		lastnamebytes = new byte[lastnamewidth];
		firstnamebytes = new byte[firstnamewidth];
		
	}

	/** 
	 * Return percentage complete 
	 * */
	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (start == end) {
	        return 0.0f;
	      } else {
	        return Math.min(1.0f, (pos - start) / (float)(end - start));
	      }
	}

	@Override
	public void close() throws IOException {
		fileIn.close();
	}

}
