package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair>
{
	private Text word1;
	private Text word2;
	private IntWritable year;
	
	public WordPair()
	{
		clear();
	}

	public WordPair(String word1,String word2,int year)
	{
		if(word2.equals("*") || word1.compareTo(word2) <= 0)
		{
			this.word1 = new Text(word1);
			this.word2 = new Text(word2);
		}
		else
		{
			this.word1 = new Text(word2);
			this.word2 = new Text(word1);
		}
		this.year = new IntWritable(year);
	}
	
	private void clear()
	{
		this.word1 = new Text();
		this.word2 = new Text();
		this.year = new IntWritable();
	}
	
	public Text getWord1()
	{
		return word1;
	}
	
	public Text getWord2()
	{
		return word2;
	}
	
	public int getYear()
	{
		return year.get();
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		word1.readFields(in);
		word2.readFields(in);
		year.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		word1.write(out);
		word2.write(out);
		year.write(out);
	}

	@Override
	public int compareTo(WordPair o)
	{
		int cmp = this.word1.compareTo(o.word1);
		if(cmp < 0)
		{
			return -1;
		}
		else if(cmp > 0)
		{
			return 1;
		}
		else
			return this.word2.compareTo(o.word2);
	}

	@Override
	public String toString()
	{
		return 	word1 + "," + word2 + "\t" +		// 0
				year;								// 1
	}

	public void reversePair()
	{
		Text temp = word1;
		word1 = word2;
		word2 = temp;
	}
	
}
