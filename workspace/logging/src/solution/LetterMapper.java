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
//logger
import org.apache.log4j.Logger;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private boolean caseSensitive = false;
	private static final Logger LOGGER = Logger.getLogger(LetterMapper.class.getName());
	
	public void setup(Context context)
	{
		//retrive configuration
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", false);
		if(conf.getBoolean("caseSensitive", false))
		{
			//true
			LOGGER.info("caseSensitive active");
			LOGGER.debug("caseSensitive active as debug information!");
			if(LOGGER.isDebugEnabled())
			{
				LOGGER.debug("useless ?");
			}
		}
		else
		{
			//false
			LOGGER.info("caseSensitive NOT actived");
			LOGGER.debug("caseSensitive NOT active as debug information!!");
			if(LOGGER.isDebugEnabled())
			{
				LOGGER.debug("still useless!");
			}
		}
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
