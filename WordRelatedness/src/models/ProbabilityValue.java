package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ProbabilityValue implements WritableComparable<ProbabilityValue>
{
	private WordPair probWordPair;
	private DoubleWritable probValue;
	private IntWritable probYear;

	public ProbabilityValue()
	{
		clear();
	}
	
	public ProbabilityValue (WordPair wordPair, DoubleWritable value, IntWritable year)
	{
		this.probWordPair = wordPair;
		this.probValue = value;
		this.probYear = year;
	}
	
	private void clear()
	{
		this.probWordPair = new WordPair();
		this.probValue = new DoubleWritable();
		this.probYear = new IntWritable();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		probWordPair.readFields(in);
		probValue.readFields(in);
		probYear.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		probWordPair.write(out);
		probValue.write(out);
		probYear.write(out);
	}

	@Override
	public int compareTo(ProbabilityValue o)
	{
		int cmp = this.probValue.compareTo(o.probValue);
		if (cmp<0)
		{
			return 1;
		}
		else 
		{
			if (cmp>0)
			{
				return -1;
			}
		}
		return cmp;
	}
	
	@Override
	public String toString()
	{
		return 	probWordPair + "\t" +
			probValue + "\t" +
			probYear + "\n";
	}
	
	public WordPair getProbWordPair()
	{
		return probWordPair;
	}

	public DoubleWritable getProbValue() 
	{
		return probValue;
	}

	public int getProbYear()
	{
		return probYear.get();
	}

	public void setProbValue(DoubleWritable probVal)
	{
		this.probValue = probVal;
	}

}
