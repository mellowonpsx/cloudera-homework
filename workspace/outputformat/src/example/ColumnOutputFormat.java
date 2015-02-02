package example;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/** 
 * 
 * @author Cloudera University
 *
 * @param <K>
 * @param <V>
 * 
 * Extends FileOutputFormat.  Very similar to TextOutputFormat but instead of separating
 * key and value with a separator character, the key string is padded with spaces to 
 * the column with specified.
 * 
 * Set KeyColumnWidth to an integer for the number of characters for the key column
 */
public class ColumnOutputFormat<K, V> extends FileOutputFormat<K, V> {

  
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {

		DataOutputStream out;
		Configuration conf = job.getConfiguration();

		if (getCompressOutput(job)) {
			// if the job output should be compressed, use a compressed output stream
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			// create the named codec
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass,
					conf);
			// build the filename including the extension
			Path file = getDefaultWorkFile(job, codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(conf);
			out = new DataOutputStream(codec.createOutputStream(fs.create(file, false)));
		} else {
			// otherwise open a stream to the default file for this task
			Path file = getDefaultWorkFile(job, "");
			FileSystem fs = file.getFileSystem(conf);
			out = fs.create(file, false);
		}
		
		/* 
		 * Create a new RecordWriter for this reducer's output stream, passing in the
		 * stream opened above and width to use for the key column.
		 */
		return new ColumnRecordWriter<K, V>(out, conf.getInt("KeyColumnWidth", 8));
	}
}
