package solution;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMonthMapper extends Mapper<LongWritable, Text, Text, Text> {

  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   *
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	  	String line = value.toString();
	  	//pattern test in TestStringRegEx (package test)
	  	Pattern ipPattern = Pattern.compile("(\\A|\\s)(?:(?:25[0-5]|2[0-4]\\d|[01]?\\d{1,2})\\.){3}(?:25[0-5]|2[0-4]\\d|[01]?\\d{1,2})(\\s|\\z)");
	  	Pattern monthPattern = Pattern.compile("(?<=\\[(([0-2]?\\d)|([3][0-2]))/)([A-Z][a-z]{2})(?=/(([1][9]{2}\\d)|([2][0][01]\\d)).*\\])");
	  	Matcher ip = ipPattern.matcher(line);
		Matcher month = monthPattern.matcher(line);
		if(ip.find() && month.find())
        {
			//String key = ip.group().trim();
			//String value = month.group().toUpperCase().trim();
			 context.write(new Text(ip.group().trim()), new Text(month.group().toLowerCase().trim()));
        }
		else
		{
			//add wrong line counter
			context.getCounter("LogMonthMapper", "skippedLines").increment(1);
		}
  }
}
