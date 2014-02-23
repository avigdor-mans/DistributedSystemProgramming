package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class ProbabilityData implements WritableComparable<ProbabilityData>
{
	private WordPair wordPair;
	private DoubleWritable probValue;

	public ProbabilityData()
	{
		clear();
	}
	
	public ProbabilityData (String word1, String word2,  int year, double value)
	{
		this.wordPair = new WordPair(word1, word2, year);
		this.probValue = new DoubleWritable(value);
	}
	
	private void clear()
	{
		this.wordPair = new WordPair();
		this.probValue = new DoubleWritable();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		wordPair.readFields(in);
		probValue.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		wordPair.write(out);
		probValue.write(out);
	}

	@Override
	public int compareTo(ProbabilityData o)
	{
		int cmp = this.probValue.compareTo(o.probValue);
		if (cmp < 0)
		{
			return 1;
		}
		else 
		{
			if (cmp > 0)
			{
				return -1;
			}
		}
		return cmp;
	}
	
	@Override
	public String toString()
	{
		return 	wordPair + "\t" +
				probValue;
	}
	
	public WordPair getProbWordPair()
	{
		return wordPair;
	}
	
	public double getProbValue() 
	{
		return probValue.get();
	}

	public int getProbYear()
	{
		return wordPair.getYear();
	}

	public void setProbValue(double probVal)
	{
		this.probValue = new DoubleWritable(probVal);
	}

}