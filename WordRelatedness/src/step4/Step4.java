package step4;

import java.io.IOException;

import models.ProbabilityData;
import models.WordPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step4
{
	public static class MapClass extends Mapper<LongWritable,Text, ProbabilityData, WordPair>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
		{
			//get information from old value
			String[] strings = value.toString().split("\t");
			String word1 = strings[0].split(",")[0];
			String word2 = strings[0].split(",")[1];
			int year = Integer.parseInt(strings[1]);

			if(word1.equals("*") || word2.equals("*"))
			{
				return;
			}

			double countW1W2 = Double.parseDouble(strings[2]);
			double countW1 = Double.parseDouble(strings[3]);
			double countW2 = Double.parseDouble(strings[4]);
			double n = Double.parseDouble(strings[5]);

			WordPair wordPair = new WordPair(word1, word2, year);
			
			//handle joint probability
			double jointVal = countW1W2 / n;
			ProbabilityData jointProbData = new ProbabilityData("joint",jointVal);
			
			context.write(jointProbData , wordPair);

			//handle dice probability
			double diceVal = (2 * countW1W2) / (countW1 + countW2);
			ProbabilityData diceProbData = new ProbabilityData("dice", diceVal);
			context.write(diceProbData , wordPair);

			//handle geometric probability
			double geometricVal = Math.sqrt(jointVal * diceVal);
			ProbabilityData geometricProbData = new ProbabilityData("geometric", geometricVal);
			context.write(geometricProbData , wordPair);
		}
	}

	public static class ReduceClass extends Reducer<ProbabilityData,WordPair,ProbabilityData,WordPair>
	{	
		@Override
		public void reduce(ProbabilityData key, Iterable<WordPair> values, Context context) throws IOException,  InterruptedException
		{
			System.out.println("in reducer");
			double k = Integer.parseInt(context.getConfiguration().get("threshold","-1"));
			
			for(WordPair value : values)
			{
				if(k > 0)
				{
					k-- ;
					String word1 = value.getWord1().toString();
					String word2 = value.getWord2().toString();
					int year = value.getYear();
					WordPair wordPair = new WordPair(word1, word2, year);
					context.write(key, wordPair);
				}
				else
				{
					break;
				}
			}
//			context.write(key, new ProbabilityData("$","$",0,k));
		}
	}

	public static class PartitionerClass extends Partitioner<ProbabilityData, WordPair>
	{
		final int DECADE = 12;
		@Override
		public int getPartition(ProbabilityData key, WordPair value, int numPartitions)
		{
			int year = value.getYear();
			int decade = year / 10;

			return decade % DECADE;
		}
	}

	public static void main(String[] args) throws Exception
	{	
		System.out.println("Start Step 4");

		Configuration conf = new Configuration();
		conf.set("threshold", args[2]);
		
		Job job = new Job(conf, "step4");
		job.setJarByClass(Step4.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(ProbabilityData.class);
		job.setOutputValueClass(WordPair.class);
		job.setNumReduceTasks(12);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}