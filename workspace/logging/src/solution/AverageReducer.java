package solution;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  int wordOccurrence = 0;
	  double totalLength = 0;
	  for (IntWritable value : values)
	  {
		  totalLength += value.get();
		  wordOccurrence++;
	  }
	  double average = totalLength/wordOccurrence;
	  context.write(key, new DoubleWritable(average));
  }
}