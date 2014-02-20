package step2;

import java.io.IOException;

import models.WordPair;
import models.WordPairData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step2
{

	public static class MapClass extends Mapper<LongWritable,Text, WordPair, WordPairData>
	{
		//private LongWritable numOfOccurences;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
		{
			//get information from old value
			String[] strings = value.toString().split("\t");
			int year = Integer.parseInt(strings[1]);
			WordPair wordPair = new WordPair(strings[0].split(",")[0],strings[0].split(",")[1],year);
			
			WordPair word1Star = new WordPair(strings[0].split(",")[0],"*",year);
			WordPair word2Star = new WordPair(strings[0].split(",")[1],"*",year);
			
			long numOfOccurences = Long.parseLong(strings[2]);
			long n = Long.parseLong(strings[5]);
			
			//init newValue + set N
			WordPairData wordPairData = new WordPairData(numOfOccurences,n);
			
			context.write(word1Star , wordPairData);
			context.write(word2Star , wordPairData);
			context.write(wordPair , wordPairData);			
		}
	}

	public static class ReduceClass extends Reducer<WordPair,WordPairData,WordPair,WordPairData>
	{	
		LongWritable countWord;
		
		@Override
		public void reduce(WordPair key, Iterable<WordPairData> values, Context context) throws IOException,  InterruptedException
		{
			long sum = 0;
			
			if(key.getWord2().toString().equals("*"))
			{
				countWord = new LongWritable(sum);
				for (WordPairData value : values)
				{
					//sums all CountWordPair in values
					sum += value.getCountWordPair();
				}
				countWord = new LongWritable(sum);
			}
			else
			{
				for(WordPairData value : values)
				{
					value.setCountWord1(countWord.get());
					context.write(key, value);
				}
			}
		}
	}

	public static class PartitionerClass extends Partitioner<WordPair, WordPairData>
	{
		final int DECADE = 12;
		@Override
		public int getPartition(WordPair key, WordPairData value, int numPartitions)
		{
			int year = key.getYear();
			int decade = year / 10; 
			return decade % DECADE;
		}
	}

	public static void main(String[] args) throws Exception
	{	
		Configuration conf = new Configuration();
//		conf.set("mapred.map.tasks","10");
//		conf.set("mapred.reduce.tasks","11");
		Job job = new Job(conf, "step2");
		job.setJarByClass(Step2.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(WordPairData.class);
		job.setNumReduceTasks(12);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
