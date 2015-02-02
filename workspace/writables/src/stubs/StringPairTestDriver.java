package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import stubs.StringPairMapper;

public class StringPairTestDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: " + this.getClass().getName() + " <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(StringPairTestDriver.class);
    job.setJobName("Custom Writable Comparable");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    /*
     * LongSumReducer is a Hadoop API class that sums values into
     * A LongWritable.  It works with any key and value type, therefore
     * supports the new StringPairWritable as a key type.
     */
    job.setReducerClass(LongSumReducer.class);

    job.setMapperClass(StringPairMapper.class);
    
    /*
	 * Set the key output class for the job
	 */   
    
    /* TODO: implement */
    
    /*
     * Set the value output class for the job
     */
    job.setOutputValueClass(LongWritable.class);


    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new StringPairTestDriver(), args);
    System.exit(exitCode);
  }
}
