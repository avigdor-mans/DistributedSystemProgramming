package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class Step2Data implements WritableComparable<Step2Data>
{
	private LongWritable countWi;
	private  <WordPairData> ListOfWordPairsData;
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public int compareTo(Step2Data o)
	{
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
