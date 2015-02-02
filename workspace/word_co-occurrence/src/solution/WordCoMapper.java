package solution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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
	    	  /*if(lastWord.length() > 0) //not efficient call lenght() method all the time ????
	    	  {
	    		  lastWord.append(',');
	    		  lastWord.append(word.toLowerCase());
		    	  context.write(new Text(lastWord.toString()), new IntWritable(1));
	    	  }*/
		    	if(firstIteration)
		    	{
		    		lastWord.append(word.toLowerCase());
		    		firstIteration = false;
		    		continue;
		    	}
		    	lastWord.append(',');
		    	lastWord.append(word.toLowerCase());
		    	context.write(new Text(lastWord.toString()), new IntWritable(1));
		    	lastWord.delete(0, lastWord.length());
		    	lastWord.append(word.toLowerCase());
	      }
	    }	    
  }
}