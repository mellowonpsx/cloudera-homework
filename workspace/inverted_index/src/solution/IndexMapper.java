package solution;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.StringBuffer;

public class IndexMapper extends Mapper<Text, Text, Text, Text>
{

  @Override
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException
  {
	  FileSplit fileSplit = (FileSplit) context.getInputSplit();
	  Path path = fileSplit.getPath();
	  String fileName = path.getName();
	  
	  String line = value.toString();
	  StringBuffer buffer = new StringBuffer();
	  buffer.append(fileName);
	  buffer.append('@');
	  buffer.append(key);
	  Text newValue = new Text(buffer.toString());
	  for (String word : line.split("\\W+"))
	  {
	      if (word.length() > 0)
	      {
	    	  context.write(new Text(word), newValue); //newvalue = fileName+"@"+key;
	      }
	  }
  }
}