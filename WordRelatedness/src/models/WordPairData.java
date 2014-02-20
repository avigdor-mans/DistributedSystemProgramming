package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class WordPairData implements WritableComparable<WordPairData>
{
	private LongWritable countWordPair;
	private LongWritable countWord1;
	private LongWritable countWord2;
	private IntWritable year;
	private LongWritable n;
	
	public WordPairData()
	{
		clear();
	}
	
	public WordPairData (WordPair wordPair, LongWritable countWordPair, IntWritable year)
	{
		this.countWordPair = countWordPair;
		this.countWord1 = new LongWritable(0);
		this.countWord2 = new LongWritable(0);
		this.year = year;
		this.n = new LongWritable(0);
	}
	
	private void clear()
	{
		this.countWordPair = new LongWritable();
		this.countWord1 = new LongWritable();
		this.countWord2 = new LongWritable();
		this.year = new IntWritable();
		this.n = new LongWritable();
	}


	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		countWordPair.readFields(in);
		countWord1.readFields(in);
		countWord2.readFields(in);
		year.readFields(in);
		n.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		countWordPair.write(out);
		countWord1.write(out);
		countWord2.write(out);
		year.write(out);
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
		return 	"CountWordPair: " + countWordPair + "\t" +
				"CountWord1: " + countWord1 + "\t" +
				"CountWord2: " + countWord2 + "\t" +
				year + "\t" + 
				n + "\n";
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
	
	public int getYear()
	{
		return year.get();
	}
	
	public LongWritable getN()
	{
		return n;
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

	public void setN(long n)
	{
		this.n = new LongWritable(n);
	}
}
