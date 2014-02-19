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
			WordPair oldWord2 = value.getWord2();
			WordPair oldWordPair = value.getWordPair();
			LongWritable oldCountWord2 = new LongWritable(value.getCountWord2().get());
			LongWritable oldNumOfOccurs = new LongWritable(value.getCountWordPair());
			LongWritable oldN = new LongWritable(value.getN().get());
			IntWritable year = new IntWritable(value.getYear());

			//create word1 and word2 from oldWordPair
			/*oldWord2 might change to word1*/
			
			
//			reminder to dror for tomorrow
//			Text wordFromKey = key.getWord2();
//			Text word1 = oldWordPair.getWord1();
//			Text word2 = oldWordPair.getWord2();

			WordPair word1 = new WordPair();
			WordPair word2 = new WordPair();
			
			//init newValue + set N
			ValueTupple newValue = new ValueTupple(word1, word2, oldWordPair, oldNumOfOccurs, year);
			newValue.setN(oldN);

			//set oldCountWord2
			if (oldWord2.compareTo(word2) == 0)
			{
				newValue.setCountWord2(oldCountWord2);
			}
			else
			{
				newValue.setCountWord1(oldCountWord2);
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
			long newCountWord1 = 0;
			long newCountWord2 = 0;

			//get information from oldValue
			ValueTupple oldValue = values.iterator().next();
			WordPair oldWord1 = oldValue.getWord1();
			WordPair oldWord2 = oldValue.getWord2();
			WordPair oldWordPair = oldValue.getWordPair();
			LongWritable oldN = new LongWritable(oldValue.getN().get());
			IntWritable year = new IntWritable(oldValue.getYear());

			for (ValueTupple value : values)
			{
				//sums all CountWordPair in values
				long occurNum = value.getCountWordPair();
				sum+=occurNum;
				//update newCountWord1
				long oldCountW1 = value.getCountWord1().get();
				if (oldCountW1 != 0){
					newCountWord1 = oldCountW1;
					//update newCountWord2
					long oldCountW2 = value.getCountWord2().get();
					if (oldCountW2 != 0){
						newCountWord2 = oldCountW2;
					}
				}

				//CountWordPair's were summed twice for each pair
				sum= sum/2;
				//create LongWritable instances for countWordPair
				LongWritable oldNumOfOccurs = new LongWritable(sum);

				//init newValue
				ValueTupple newValue = new ValueTupple(oldWord1, oldWord2, oldWordPair, oldNumOfOccurs, year);

				//create LongWritable instances for countWord1 countWord2
				LongWritable countWord1 = new LongWritable(newCountWord1);
				LongWritable countWord2 = new LongWritable(newCountWord2);

				//set newValue
				newValue.setN(oldN); 
				newValue.setCountWord1(countWord1);
				newValue.setCountWord2(countWord2);

				context.write(oldWordPair , newValue);
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
