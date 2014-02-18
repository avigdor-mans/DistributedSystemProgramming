package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class ValueTupple implements WritableComparable<ValueTupple>
{
	private WordPair word1;
	private LongWritable countWord1;
	private WordPair word2;
	private LongWritable countWord2;
	private WordPair wordPair;
	private LongWritable countWordPair;
	private IntWritable year;
	private LongWritable n;

	public ValueTupple()
	{
		super();
		System.out.println("empty tupple");
	}

	public ValueTupple (WordPair word1, WordPair word2,	WordPair wordPair,
			LongWritable countWordPair, IntWritable year)
	{
		super();
		this.word1 = word1;
		this.countWord1 = new LongWritable(0);
		this.word2 = word2;
		this.countWord2 = new LongWritable(0);
		this.wordPair = wordPair;
		this.countWordPair = countWordPair;
		this.year = year;
		this.n = new LongWritable(0);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		word1.readFields(in);
		countWord1.readFields(in);
		word2.readFields(in);
		countWord2.readFields(in);
		wordPair.readFields(in);
		countWordPair.readFields(in);
		year = new IntWritable(in.readInt());
		n = new LongWritable(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		word1.write(out);
		countWord1.write(out);
		word2.write(out);
		countWord2.write(out);
		wordPair.write(out);
		countWordPair.write(out);
		year.write(out);
		n.write(out);
	}

	@Override
	public int compareTo(ValueTupple o)
	{
		return this.wordPair.compareTo(o.wordPair);
	}

	public WordPair getWordPair()
	{
		return wordPair;
	}

	@Override
	public String toString()
	{
		return 	word1 + "\t" +
				countWord1 + "\t" +
				word2 + "\t" +
				countWord2 + "\t" +
				wordPair + "\t" + 
				countWordPair + "\t" + 
				year + "\t" + 
				n;
	}

	public LongWritable getCountWord1() {
		return countWord1;
	}

	public LongWritable getCountWord2() {
		return countWord2;
	}

	public long getCountWordPair() {
		return countWordPair.get();
	}

//	public void setCount(WordPair key,long sum)
//	{
//		Text star = new Text("*");
//		if(key.getWord1().compareTo(star) == 0)
//		{
//			if(key.getWord2().compareTo(star) == 0)
//			{
//				// N <- count
//				n.set(sum);
//			}
//			else if(key.compareTo(word1) == 0)
//			{
//				// word1 <- count
//				countWord1.set(sum);
//			}
//			else
//			{
//				// word2 <- count
//				countWord2.set(sum);
//			}
//		}
//		else
//		{
//			// wordPair <- count
//			countWordPair.set(sum);
//		}
//		
//	}
	
	public LongWritable getN() {
		return n;
	}

	public WordPair getWord1() {
		return word1;
	}

	public WordPair getWord2() {
		return word2;
	}

	public int getYear()
	{
		return year.get();
	}

	public void setCountWord1(LongWritable countWord1) {
		this.countWord1 = countWord1;
	}

	public void setCountWord2(LongWritable countWord2) {
		this.countWord2 = countWord2;
	}

	public void setCountWordPair(LongWritable countWordPair) {
		this.countWordPair = countWordPair;
	}

	public void setN(LongWritable n) {
		this.n = n;
	}
}
