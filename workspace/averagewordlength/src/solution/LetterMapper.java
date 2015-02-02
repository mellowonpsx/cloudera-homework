package solution;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper
extends Mapper<LongWritable, Text, Text, IntWritable>
{
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException
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
				context.write(new Text(word.toUpperCase().substring(0, 1)), new IntWritable(word.toUpperCase().length()));
			}
		}
	}
}
