package extracredit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCoAscendingFrequency extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: WordCo <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(WordCoAscendingFrequency.class);
    job.setJobName("Word Co-Occurrence String Pair");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(SequenceFileInputFormat.class);

    //job.setMapOutputKeyClass(IntWritable.class);
    //job.setMapOutputValueClass(StringPairWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(StringPairWritable.class);
    job.setMapperClass(InverseMapper.class);
    //job.setReducerClass(SumReducer.class);//identity reducer

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new WordCoAscendingFrequency(), args);
    System.exit(exitCode);
  }
}
