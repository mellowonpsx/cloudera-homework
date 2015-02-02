package example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NameYearPartitioner<K2, V2> extends
		HashPartitioner<StringPairWritable, Text> {

	/**
	 * Partition Name/Year pairs according to the first string (last name) in the string pair so 
	 * that all keys with the same last name go to the same reducer, even if  second part
	 * of the key (birth year) is different.
	 */
	public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {
		return (key.getLeft().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}
