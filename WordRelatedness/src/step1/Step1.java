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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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

			int numOfWords = numOfTokens - 4;
			
			System.out.println("num of words is: " + numOfWords);
			
			ArrayList<String> words = new ArrayList<String>();

			for(int i = 0 ; i < numOfWords ; i++)
			{
				String word = itr.nextToken().replace("\"","").replace("'", "").trim();
				System.out.println(word);
				if(!word.isEmpty() && !StopWords.isStopWord(word.toLowerCase()))
				{
					words.add(word);
					System.out.println("words size: " + words.size());
				}
			}
			
			if(words.size()  < 2)
			{
				return;
			}
			else
			{
				numOfWords = words.size();
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
				//Adding keys for pair: (middleWord,word)
				WordPair emptyPair = new WordPair("*","*",year.get());
				WordPairData emptyWordPairData = new WordPairData(numOfOccurences.get());
				context.write(emptyPair , emptyWordPairData);

				WordPair wordPair = new WordPair(middleWord,word,year.get());
				WordPairData wordPairData = new WordPairData(numOfOccurences.get());
				context.write(wordPair , wordPairData);
			}			
		}
	}

	public static class ReduceClass extends Reducer<WordPair,WordPairData,WordPair,WordPairData>
	{
		LongWritable n = new LongWritable(-1);

		@Override
		public void reduce(WordPair key, Iterable<WordPairData> values, Context context) throws IOException,  InterruptedException
		{
			if(key.getWord1().toString().equals("*") && key.getWord2().toString().equals("*"))
			{
				long sum = 0;
				for (WordPairData value : values)
				{
					sum += value.getCountWordPair();
				}
				n = new LongWritable(sum);
				context.write(key, new WordPairData(sum,n.get()));
			}			
			else
			{
				long sum = 0;
				for (WordPairData value : values)
				{
					sum += value.getCountWordPair();
				}
				WordPairData newWordPairData = new WordPairData(sum,n.get());				
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
			int year = key.getYear();
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
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(WordPairData.class);
		job.setNumReduceTasks(12);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

