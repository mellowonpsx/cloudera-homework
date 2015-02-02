package test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import org.junit.Before;
import org.junit.Test;

// needed for list
import solution.CountReducer;
import solution.LogMonthMapper;

public class TestProcessLogs 
{
	MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable> mapReduceDriver;
  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp()
  {

    mapReduceDriver = new MapReduceDriver<LongWritable, Text,  Text, Text, Text, IntWritable>();
    mapReduceDriver.withMapper(new LogMonthMapper());
    mapReduceDriver.withReducer(new CountReducer());
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMonthPartitionerFail()
  {
	  mapReduceDriver.withInput(new LongWritable(1), new Text("96.7.4.14 psodfkspo - - [24/Apasdr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433"));
	  mapReduceDriver.withOutput(new Text("96.7.4.14"), new IntWritable(1));
	  mapReduceDriver.runTest();
  }
  
  @Test
  public void testMonthPartitioner()
  {
	  mapReduceDriver.withInput(new LongWritable(1), new Text("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433"));
	  mapReduceDriver.withInput(new LongWritable(1), new Text("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433"));
	  mapReduceDriver.withInput(new LongWritable(1), new Text("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433"));
	  mapReduceDriver.withInput(new LongWritable(1), new Text("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433"));
	  mapReduceDriver.withOutput(new Text("96.7.4.14"), new IntWritable(4));
	  mapReduceDriver.runTest();
  }
}
