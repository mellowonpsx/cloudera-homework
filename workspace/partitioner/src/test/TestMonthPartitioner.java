package test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import solution.MonthPartitioner;

public class TestMonthPartitioner
{
	public static void main(String args[])
	{
		MonthPartitioner <Text,Text>monthPartitioner = new MonthPartitioner<Text, Text>();
		monthPartitioner.setConf(new Configuration());
		
		String[] value = { "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec" }; 
		for (String s : value)
		{
			int res = monthPartitioner.getPartition(new Text(s), new Text(s), 12);
		    System.out.println("month: "+s+" reducer: "+res);
		}
	}
}
