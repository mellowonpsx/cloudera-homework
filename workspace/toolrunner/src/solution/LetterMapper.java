package solution;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//tool runner
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private boolean caseSensitive = false;
	
	public void setup(Context context)
	{
		//retrive configuration
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", false);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// take line as input
		String line = value.toString();
		//split word with regEx
		for (String word : line.split("\\W+"))
		{
			//if is not blank (double spaces etc)
			if (word.length() > 0)
			{
				//write one word with his length
				//context.write(new Text(word.substring(0, 1)), new IntWritable(word.length()));
				if(!caseSensitive)
				{
					word = word.toLowerCase();
				}
				context.write(new Text(word.substring(0, 1)), new IntWritable(word.length()));
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		
	}
}
