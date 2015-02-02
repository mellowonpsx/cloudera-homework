package example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 
 * 
 * @author Cloudera University
 * 
 * This class demonstrates the use of using custom comparators to do secondary sorting.
 * Job input is a text file containing lines in the following format:
 *    Lastname Firstname YYYY-Mon-DD [additional text data]
 * The job will sort the data by last name (ascending) and year (descending), grouped
 * by Lastname/Year.
 *
 */

public class NameYearDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitcode = ToolRunner.run(new Configuration(),
				new NameYearDriver(), args);
		System.exit(exitcode);
	}

	public int run(String[] args) throws Exception {
		

		if (args.length != 2) {
			System.out.printf("Usage: " + this.getClass().getName() +  "<input dir> <output dir>\n");
			System.exit(-1);
		}
		

		Job job = new Job(getConf());
		job.setJarByClass(NameYearDriver.class);

		job.setJobName("Name Year Sort");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(NameYearMapper.class);
		//job.setReducerClass(NameYearReducer.class);

		/*
		 * The Mapper outputs StringPair objects in which the first string
		 * is the Last Name field of the record, and the second string is
		 * the Birth Year field of the record.
		 */
		job.setMapOutputKeyClass(StringPairWritable.class);
		job.setMapOutputValueClass(Text.class);		

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		/* 
		 * set sort comparator class so that name/year keys are
		 * sorted first in ascending order by name, then descending order by year 
		 */
		//job.setSortComparatorClass(NameYearComparator.class);
		
		/* 
		 * set the grouping comparator class so that all name/year keys
		 * with the same name are grouped into the same call to the 
		 * reduce method
		 */
		//job.setGroupingComparatorClass(NameComparator.class);

		/*
		 * set custom partitioner so that string pair keys with the same 
		 * last name go to the same reducer.
		 */
		//job.setPartitionerClass(NameYearPartitioner.class);
		
		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
