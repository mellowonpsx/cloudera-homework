package example;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** 
 * 
 * @author Cloudera University
 *
 * @param <K>
 * @param <V>
 * 
 * Write out a record.  Functions similarly to the record writer for TextOutputFormat 
 * except that instead of a separater character between the key and value, the key
 * is converted to a string which is padded with spaces out to the length specified
 * in the columnwidth parameter.
 */
public class ColumnRecordWriter<K,V> extends RecordWriter<K, V> {

	private DataOutputStream out;
	private int columnWidth;
	
	public ColumnRecordWriter(DataOutputStream out, int columnWidth) {
		this.out = out;
		this.columnWidth = columnWidth;
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		out.close();
		
	}

	@Override
	public void write(K key, V value) throws IOException,
			InterruptedException {
		// format output string by padding the key string out to the required number of characters
		// (minus sign indicates left justified, spaces go at end of string)
		String outstring = String.format("%-" + columnWidth + "s%s\n",key.toString(),value.toString());
		out.writeBytes(outstring);
		
	}


}
