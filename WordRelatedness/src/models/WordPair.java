package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair>
{
	private Text word1;
	private Text word2;
	
	public WordPair()
	{
		this.word1 = new Text("*");
		this.word2 = new Text("*");
	}

	public WordPair(String word1,String word2)
	{
		super();
		if(word1.compareTo(word2) <= 0)
		{
			this.word1 = new Text(word1);
			this.word2 = new Text(word2);
		}
		else
		{
			this.word1 = new Text(word2);
			this.word2 = new Text(word1);
		}
	}
	
	private void clear()
	{
		this.word1 = new Text();
		this.word2 = new Text();
	}
	
	public Text getWord1()
	{
		return word1;
	}
	
	public Text getWord2()
	{
		return word2;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		word1.readFields(in);
		word2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		word1.write(out);
		word2.write(out);
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
		return word1 + "\t" + word2;
	}

}
