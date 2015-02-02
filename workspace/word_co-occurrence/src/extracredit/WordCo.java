package extracredit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCo extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: WordCo <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(WordCo.class);
    job.setJobName("Word Co-Occurrence String Pair");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

    //job.setInputFormatClass(StringPairWritable.class);
    //job.setMapOutputKeyClass(StringPairWritable.class);
    //job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(StringPairWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(WordCoMapper.class);
    job.setReducerClass(SumReducer.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new WordCo(), args);
    System.exit(exitCode);
  }
}
