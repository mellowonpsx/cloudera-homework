package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
//import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

import solution.LetterMapper;

// needed for list
//import java.util.ArrayList;
//import java.util.List;

public class TestToolRunnerParametrizedMapper
{
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	public boolean caseSensitive = true;
	@Before
	public void setUp()
	{
		Configuration conf = new Configuration();
		conf.setBoolean("caseSensitive", caseSensitive);
		
		LetterMapper mapper = new LetterMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();;
		mapDriver.setMapper(mapper);
		mapDriver.withConfiguration(conf);
	}
	  
	@Test
	public void testMapper()
	{
		mapDriver.withInput(new LongWritable(1), new Text("Cat cat Dog dog"));
		if(!caseSensitive)
		{
			mapDriver.addOutput(new Text("c"), new IntWritable(3));
			mapDriver.addOutput(new Text("c"), new IntWritable(3));
			mapDriver.addOutput(new Text("d"), new IntWritable(3));
			mapDriver.addOutput(new Text("d"), new IntWritable(3));
		}
		else
		{
			mapDriver.addOutput(new Text("C"), new IntWritable(3));
			mapDriver.addOutput(new Text("c"), new IntWritable(3));
			mapDriver.addOutput(new Text("D"), new IntWritable(3));
			mapDriver.addOutput(new Text("d"), new IntWritable(3));
		}
		mapDriver.runTest();  
	}
}


/*
package solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

// needed for list
//import java.util.ArrayList;
//import java.util.List;

public class TestToolRunnerParam
{
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	
	public boolean caseSensitive = false;
	@Before
	public void setUp()
	{
		LetterMapper mapper = new LetterMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();;
		mapDriver.setMapper(mapper);
		
		AverageReducer reducer = new AverageReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
	    reduceDriver.setReducer(reducer);
	    
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	    
	    //add configuration
	    Configuration conf = new Configuration();
		conf.setBoolean("caseSensitive", caseSensitive);
		mapReduceDriver.withConfiguration(conf);
	}
	  
	@Test
	public void testMapReduce()
	{

		mapReduceDriver.withInput(new LongWritable(1), new Text("Ca cata Do doga"));
		if(!caseSensitive)
		{
			mapReduceDriver.addOutput(new Text("c"), new DoubleWritable(10));
			mapReduceDriver.addOutput(new Text("d"), new DoubleWritable(10));
		}
		else
		{
			//mapReduceDriver.addOutput(new Text("C"), new DoubleWritable(2));
			//mapReduceDriver.addOutput(new Text("c"), new DoubleWritable(4));
			//mapReduceDriver.addOutput(new Text("D"), new DoubleWritable(2));
			//mapReduceDriver.addOutput(new Text("d"), new DoubleWritable(4));
		}
	}
}
*/