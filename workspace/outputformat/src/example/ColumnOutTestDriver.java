package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ColumnOutTestDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		int exitCode = ToolRunner.run(conf, new ColumnOutTestDriver(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: " + this.getClass().getName() + " <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job(getConf());
		job.setJarByClass(ColumnOutTestDriver.class);
		job.setJobName("Output Format Example");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputFormatClass(ColumnOutputFormat.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);
	}

}
