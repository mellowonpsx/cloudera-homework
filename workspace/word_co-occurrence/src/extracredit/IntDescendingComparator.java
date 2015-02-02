package extracredit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;


public class IntDescendingComparator extends WritableComparator
{
	public IntDescendingComparator()
	{
		super(IntWritable.class);
	}
	    
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	{
	      int thisValue = readInt(b1, s1);
	      int thatValue = readInt(b2, s2);
	      return (thisValue<thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
	}
}