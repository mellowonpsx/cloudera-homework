package stubs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

// needed for list
import java.util.ArrayList;
import java.util.List;

public class TestWordCount {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
    WordMapper mapper = new WordMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    SumReducer reducer = new SumReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

    /*
     * For this test, the mapper's input will be "1 cat cat dog" 
     * TODO: implement
     */
	  mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
	  mapDriver.addOutput(new Text("cat"), new IntWritable(1));
	  mapDriver.addOutput(new Text("cat"), new IntWritable(1));
	  mapDriver.addOutput(new Text("dog"), new IntWritable(1));
	  mapDriver.runTest();
	  //fail("Please implement test.");
	  
  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {

    /*
     * For this test, the reducer's input will be "cat 1 1".
     * The expected output is "cat 2".
     * TODO: implement
     */
	  List<IntWritable> resultValues = new ArrayList<IntWritable>();
	  resultValues.add(new IntWritable(1));
	  resultValues.add(new IntWritable(1));
	  reduceDriver.withInput(new Text("cat"), resultValues);
	  reduceDriver.addOutput(new Text("cat"), new IntWritable(2));
	  reduceDriver.runTest();
	  //fail("Please implement test.");
  }


  /*
   * Test the mapper and reducer working together.
   */
  @Test
  public void testMapReduce() {

    /*
     * For this test, the mapper's input will be "1 cat cat dog" 
     * The expected output (from the reducer) is "cat 2", "dog 1". 
     * TODO: implement
     */
	  mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
	  mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
	  mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
	  //fail("Please implement test.");
  }
}
