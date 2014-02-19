package step1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import models.StopWords;
import models.ValueTupple;
import models.WordPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class Step1
{
	public static class MapClass extends Mapper<LongWritable, Text, WordPair, ValueTupple>
	{
		private IntWritable year;
		private LongWritable numOfOccurences;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
		{
			System.out.println("in map1");
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			//check enough tokens were recieved
			int numOfTokens = itr.countTokens();
			
			if (numOfTokens < 6)
			{
				System.out.println("Amount of tokens is to small");
				return;
			}
			
			int numOfWords = numOfTokens - 4;
			ArrayList<String> words = new ArrayList<>();
			
			for(int i = 0 ; i < numOfWords ; i++)
			{
				words.add(itr.nextToken());
			}
			
			//midWordIndex in case of- 5word:2, 4word:2, 3word:1, 2word:1
			int midWordIndex;
			midWordIndex = numOfWords/2;
			
			String middleWord = words.remove(midWordIndex);
			
			System.out.println("middle word is: " + middleWord);
						
			String yearAsString = itr.nextToken();
			year = new IntWritable(Integer.parseInt(yearAsString));
			
			if(year.get() < 1900)
			{
				return;
			}
			
			String numOfOccurencesAsString = itr.nextToken();
			numOfOccurences = new LongWritable(Long.parseLong(numOfOccurencesAsString));
			
			for(String word : words)
			{
				if(!StopWords.isStopWord(word))
				{
					System.out.println("Adding keys for pair: (" + middleWord +"," + word + ")");
					
					WordPair emptyPair = new WordPair();
					WordPair wordPair = new WordPair(middleWord,word);
					WordPair word1 = new WordPair(middleWord,"*");
					WordPair word2 = new WordPair(word,"*");
					ValueTupple valueTupple = new ValueTupple(word1, word2, wordPair, numOfOccurences, year);
												
					context.write(emptyPair , valueTupple);
					context.write(word1 , valueTupple);
					context.write(word2 , valueTupple);
				}
			}			
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
				ValueTupple oldValue = null;
				boolean gotOldValue = false;
				for (ValueTupple value : values)
				{
					if(!gotOldValue)
					{
						oldValue = value;
						gotOldValue = true;
					}
					long occurNum = value.getCountWordPair();
					sum+=occurNum;
				}
				
				WordPair word1 = oldValue.getStarWord1();
				WordPair word2 = oldValue.getStarWord2();
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
		System.out.println("Start Step 1");
		Configuration conf = new Configuration();
//		conf.set("mapred.map.tasks","10");
//		conf.set("mapred.reduce.tasks","11");
		Job job = new Job(conf, "step1");
		job.setJarByClass(Step1.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(ValueTupple.class);
		job.setNumReduceTasks(12);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

