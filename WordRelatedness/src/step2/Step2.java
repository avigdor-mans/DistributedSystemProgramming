package step2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import models.StopWords;
import models.ValueTupple;
import models.WordPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Step2 {

	public static class MapClass extends Mapper<WordPair,ValueTupple, WordPair, ValueTupple>
	{
		//private IntWritable year;
		//private LongWritable numOfOccurences;

		@Override
		public void map(WordPair key, ValueTupple value, Context context) throws IOException,  InterruptedException
		{
			
		}
	}
	
	public static class ReduceClass extends Reducer<WordPair,ValueTupple,WordPair,ValueTupple>
	{
		static long SumN = 0;
		
		public boolean isStarPair (WordPair key){
			boolean ans= ((key.getWord1().compareTo(new Text("*")) == 0) && (key.getWord2().compareTo(new Text("*")) == 0));
			return ans;
		}
		
		@Override
		public void reduce(WordPair key, Iterable<ValueTupple> values, Context context) throws IOException,  InterruptedException
		{
			long sum = 0;
			
			//handle key(*,*)
			if (isStarPair(key)){
				for (ValueTupple value : values){
					SumN+=1;
				}
			}
			
			//handle key(*,z)
			else {
				for (ValueTupple value : values){
					long occurNum = value.getCountWordPair();
					sum+=occurNum;
				}
				ValueTupple oldValue = values.iterator().next();
				WordPair word1 = oldValue.getWord1();
				WordPair word2 = oldValue.getWord2();
				WordPair wordPair = oldValue.getWordPair();
				LongWritable numOfOccurences = new LongWritable(oldValue.getCountWordPair());
				IntWritable year = new IntWritable(oldValue.getYear());
				ValueTupple newValue = new ValueTupple(word1, word2, wordPair, numOfOccurences, year);			
				newValue.setCountWord2(new LongWritable(sum));
				newValue.setN(new LongWritable(SumN));
				
				context.write(key, newValue);	
			}
		}
	}

	public static class PartitionerClass extends Partitioner<WordPair, ValueTupple>
	{
		final int DECADE = 11;
		@Override
		public int getPartition(WordPair key, ValueTupple value, int numPartitions)
		{
			int year = value.getYear();
			int decade = year / 10; 
			return decade % DECADE;
		}
	}

	public static void main(String[] args) throws Exception
	{	
		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","10");
		conf.set("mapred.reduce.tasks","11");
		Job job = new Job(conf, "step2");
		job.setJarByClass(Step2.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
