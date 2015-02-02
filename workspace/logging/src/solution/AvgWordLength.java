package solution;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//tool runner
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AvgWordLength extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{		
		int exitCode = ToolRunner.run(new Configuration(),new AvgWordLength(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.out.printf("Usage: %s [-D caseSensitive=true|false] <input dir> <output dir>\n", getClass().getSimpleName());
			return -1;
		}
		
		//default configuration
		Configuration conf = this.getConf();
		Job job = new Job(conf);
		//Job job = new Job();
		
		job.setJarByClass(AvgWordLength.class);
		job.setJobName("Average Word Length");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

    
    //map and reducer
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);
        
    //input and output value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    return (success ? 0 : 1);
  }
}