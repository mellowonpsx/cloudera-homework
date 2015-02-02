package extracredit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, StringPairWritable, IntWritable>
{

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
	    String line = value.toString();
	    StringBuffer lastWord = new StringBuffer();
	    boolean firstIteration = true;
	    for(String word : line.split("\\W+"))
	    {
	      if (word.length() > 0)
	      {
		    	if(firstIteration)
		    	{
		    		lastWord.append(word.toLowerCase());
		    		firstIteration = false;
		    		continue;
		    	}
		    	context.write(new StringPairWritable(lastWord.toString(), word.toLowerCase()), new IntWritable(1));
		    	lastWord.delete(0, lastWord.length());
		    	lastWord.append(word.toLowerCase());
	      }
	    }	
  }
}
