package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StringPairMapper extends
		Mapper<LongWritable, Text, StringPairWritable, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		LongWritable one = new LongWritable(1);
		/*
		 * Split the line into words. Create a new StringPairWritable consisting
		 * of the first two strings in the line.  Emit the pair as the key, and
		 * '1' as the value (for later summing).
		 */
		String[] words = value.toString().split("\\W+", 3);

		if (words.length > 2) {
			context.write(new StringPairWritable(words[0], words[1]), one);
		}
	}
}
