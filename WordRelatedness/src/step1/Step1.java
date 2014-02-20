package step1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import models.StopWords;
import models.WordPairData;
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
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class Step1
{
	public static class MapClass extends Mapper<LongWritable, Text, WordPair, WordPairData>
	{
		private IntWritable year;
		private LongWritable numOfOccurences;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
		{
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
				String word = itr.nextToken().replace("\"", "");
				if(!word.isEmpty())
				{
					words.add(word.toLowerCase());
				}
			}

			//midWordIndex in case of- 5word:2, 4word:2, 3word:1, 2word:1
			int midWordIndex;
			midWordIndex = numOfWords/2;

			String middleWord = words.remove(midWordIndex);

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
					//Adding keys for pair: (middleWord,word)

					WordPair emptyPair = new WordPair("*","*");
					WordPairData emptyWordPairData = new WordPairData(emptyPair,numOfOccurences, year);
					context.write(emptyPair , emptyWordPairData);
					
					WordPair wordPair = new WordPair(middleWord,word);
					WordPairData wordPairData = new WordPairData(wordPair, numOfOccurences, year);
					context.write(wordPair , wordPairData);
				}
			}			
		}
	}

	public static class ReduceClass extends Reducer<WordPair,WordPairData,WordPair,WordPairData>
	{
		static long n;
		static long sum;
		
		@Override
		public void reduce(WordPair key, Iterable<WordPairData> values, Context context) throws IOException,  InterruptedException
		{
			IntWritable year = null;
			boolean gotDecade = false;
			sum = 0;
			
			for (WordPairData value : values)
			{
				if(!gotDecade)
				{
					year = new IntWritable(value.getYear());
					gotDecade = true;
				}
				// in case of (*,*) sums 1's , else sums the numOfOccurences
				sum += value.getCountWordPair();
			}
			
			if(key.getWord1().toString().equals("*") && key.getWord2().toString().equals("*"))
			{
				n = sum;
				return;
			}
			else
			{
				LongWritable totalSum = new LongWritable(sum);
				WordPairData newWordPairData = new WordPairData(key,totalSum, year);
				
				newWordPairData.setN(n);
				context.write(key, newWordPairData);
			}
		}
	}

	public static class PartitionerClass extends Partitioner<WordPair, WordPairData>
	{
		final int DECADE = 12;
		@Override
		public int getPartition(WordPair key, WordPairData value, int numPartitions)
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
		Job job = new Job(conf, "step1");
		job.setJarByClass(Step1.class);
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

