package solution;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner<K2, V2> extends Partitioner<Text, Text> implements Configurable
{

  private Configuration configuration;
  HashMap<String, Integer> months = new HashMap<String, Integer>();

  /**
   * Set up the months hash map in the setConf method.
   */
  @Override
  public void setConf(Configuration configuration)
  {
	  this.configuration = configuration;
	  months.put("jan", 0);
	  months.put("feb", 1);
	  months.put("mar", 2);
	  months.put("apr", 3);
	  months.put("may", 4);
	  months.put("jun", 5);
	  months.put("jul", 6);
	  months.put("aug", 7);
	  months.put("sep", 8);
	  months.put("oct", 9);
	  months.put("nov", 10);
	  months.put("dec", 11);
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf()
  {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the three-letter abbreviation for the month
   * as its value. (It is the output value from the mapper.)
   * It should return an integer representation of the month.
   * Note that January is represented as 0 rather than 1.
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 12 reducers.
   */
  public int getPartition(Text key, Text value, int numReduceTasks)
  {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the month
     * represented by the value parameter.  Use to months hashtable to map
     * the string to the month number: months.get(value.toString()) and cast it to int.
     */
     return (int)months.get(value.toString())%numReduceTasks; //prevent problem in case of less than 12 reducer
  }
}
