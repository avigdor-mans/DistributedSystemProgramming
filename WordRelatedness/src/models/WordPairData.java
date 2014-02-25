package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class WordPairData implements WritableComparable<WordPairData>
{
//	private WordPair wordPair;
	private LongWritable countWordPair;
	private LongWritable countWord1;
	private LongWritable countWord2;
	private LongWritable n;
	
	public WordPairData()
	{
		clear();
	}
	
	public WordPairData (long countWordPair)
	{
		this.countWordPair = new LongWritable(countWordPair);
		this.countWord1 = new LongWritable(0);
		this.countWord2 = new LongWritable(0);
		this.n = new LongWritable(0);
	}
	
	public WordPairData (long countWordPair, long n)
	{
		this.countWordPair = new LongWritable(countWordPair);
		this.countWord1 = new LongWritable(0);
		this.countWord2 = new LongWritable(0);
		this.n = new LongWritable(n);
	}
	
	private void clear()
	{
		this.countWordPair = new LongWritable();
		this.countWord1 = new LongWritable();
		this.countWord2 = new LongWritable();
		this.n = new LongWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		countWordPair.readFields(in);
		countWord1.readFields(in);
		countWord2.readFields(in);
		n.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		countWordPair.write(out);
		countWord1.write(out);
		countWord2.write(out);
		n.write(out);
	}

	@Override
	public int compareTo(WordPairData o)
	{
		return 0;
	}

	@Override
	public String toString()
	{
		return 	countWordPair + "\t" +		// 2
				countWord1 + "\t" +			// 3
				countWord2 + "\t" +			// 4
				n ;							// 5
	}
	
	public long getCountWordPair()
	{
		return countWordPair.get();
	}
	
	public long getCountWord1()
	{
		return countWord1.get();
	}
	
	public long getCountWord2()
	{
		return countWord2.get();
	}
	
	public long getN()
	{
		return n.get();
	}

	public void setCountWordPair(long countWordPair)
	{
		this.countWordPair = new LongWritable(countWordPair);
	}
	
	public void setCountWord1(long countWord1)
	{
		this.countWord1 = new LongWritable(countWord1);
	}
	
	public void setCountWord2(long countWord2)
	{
		this.countWord2 = new LongWritable(countWord2);
	}

	public void setN(LongWritable n)
	{
		this.n = n;
	}
}
