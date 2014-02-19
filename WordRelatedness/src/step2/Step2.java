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

public class Step2
{

	public static class MapClass extends Mapper<WordPair,ValueTupple, WordPair, ValueTupple>
	{
		//private IntWritable year;
		//private LongWritable numOfOccurences;

		@Override
		public void map(WordPair key, ValueTupple value, Context context) throws IOException,  InterruptedException
		{
			//get information from old value
			WordPair starWord1 = value.getStarWord1();
			WordPair starWord2 = value.getStarWord2();

			WordPair oldWordPair = value.getWordPair();
			LongWritable oldNumOfOccurs = new LongWritable(value.getCountWordPair());

			LongWritable oldN = new LongWritable(value.getN().get());
			IntWritable year = new IntWritable(value.getYear());

			//init newValue + set N
			ValueTupple newValue = new ValueTupple(starWord1, starWord2, oldWordPair, oldNumOfOccurs, year);
			newValue.setN(oldN);

			// set oldCountWord
			if (starWord1.compareTo(key) == 0)
			{
				LongWritable oldCountStarWord1 = new LongWritable(value.getCountWord1().get());
				newValue.setCountWord1(oldCountStarWord1);
			}
			else
			{
				LongWritable oldCountStarWord2 = new LongWritable(value.getCountWord2().get());
				newValue.setCountWord2(oldCountStarWord2);
			}
			context.write(oldWordPair , newValue);
		}
	}

	public static class ReduceClass extends Reducer<WordPair,ValueTupple,WordPair,ValueTupple>
	{	
		@Override
		public void reduce(WordPair key, Iterable<ValueTupple> values, Context context) throws IOException,  InterruptedException
		{
			long sum = 0;
			long newCountStarWord1 = 0;
			long newCountStarWord2 = 0;

			ValueTupple oldValue = null;
			boolean gotOldValue = false;

			for (ValueTupple value : values)
			{
				if(!gotOldValue)
				{
					oldValue = value;
					gotOldValue = true;
				}

				//sums all CountWordPair in values
				long occurNum = value.getCountWordPair();
				sum+=occurNum;

				//update newCountWord1
				long oldCountW1 = value.getCountWord1().get();
				if (oldCountW1 != 0){
					newCountStarWord1 = oldCountW1;
				}	

				//update newCountWord2
				else{	
					long oldCountW2 = value.getCountWord2().get();
					if (oldCountW2 != 0){
						newCountStarWord2 = oldCountW2;
					}
				}
			}

			//CountWordPair's were summed twice for each pair
			sum= sum/2;
			//create LongWritable instances for countWordPair
			LongWritable newNumOfOccurs = new LongWritable(sum);

			//get information from oldValue
			WordPair oldWord1 = oldValue.getStarWord1();
			WordPair oldWord2 = oldValue.getStarWord2();
			WordPair oldWordPair = oldValue.getWordPair();
			LongWritable oldN = new LongWritable(oldValue.getN().get());
			IntWritable year = new IntWritable(oldValue.getYear());

			//init newValue
			ValueTupple newValue = new ValueTupple(oldWord1, oldWord2, oldWordPair, newNumOfOccurs, year);

			//create LongWritable instances for countWord1 countWord2
			LongWritable countWord1 = new LongWritable(newCountStarWord1);
			LongWritable countWord2 = new LongWritable(newCountStarWord2);

			//set newValue
			newValue.setN(oldN); 
			newValue.setCountWord1(countWord1);
			newValue.setCountWord2(countWord2);

			context.write(oldWordPair , newValue);
		}
	}

	public static class PartitionerClass extends Partitioner<WordPair, ValueTupple>
	{
		final int DECADE = 12;
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
//		conf.set("mapred.map.tasks","10");
//		conf.set("mapred.reduce.tasks","11");
		Job job = new Job(conf, "step2");
		job.setJarByClass(Step2.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(12);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
