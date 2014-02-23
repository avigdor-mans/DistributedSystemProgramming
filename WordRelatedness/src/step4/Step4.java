package step4;

import java.io.IOException;

import models.ProbabilityData;

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

public class Step4
{
	static Integer kFromArgs = new Integer(-1);

	public static class MapClass extends Mapper<LongWritable,Text, Text, ProbabilityData>
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

			//handle joint probability
			double jointVal = countW1W2 / n;
			ProbabilityData jointProbData = new ProbabilityData(word1, word2, year, jointVal);
			Text joint = new Text("joint");
			context.write(joint , jointProbData);

			//handle dice probability
			double diceVal = (2 * countW1W2) / (countW1 + countW2);
			ProbabilityData diceProbData = new ProbabilityData(word1, word2, year, diceVal);
			Text dice = new Text("dice");
			context.write(dice , diceProbData);

			//handle geometric probability
			double geometricVal = Math.sqrt(jointVal * diceVal);
			ProbabilityData geometricProbData = new ProbabilityData(word1, word2, year, geometricVal);
			Text geometric = new Text("geometric");
			context.write(geometric , geometricProbData);
			
		}
	}

	public static class ReduceClass extends Reducer<Text,ProbabilityData,Text,ProbabilityData>
	{	
		@Override
		public void reduce(Text key, Iterable<ProbabilityData> values, Context context) throws IOException,  InterruptedException
		{
			int k = Step4.kFromArgs.intValue();

			for(ProbabilityData value : values)
			{
				//				if(k > 0)
				//				{
				//					k-- ;
				String word1 = value.getProbWordPair().getWord1().toString();
				String word2 = value.getProbWordPair().getWord2().toString();
				double probValue = value.getProbValue();
				ProbabilityData probabilityData = new ProbabilityData(word1, word2, value.getProbYear(), probValue);
				context.write(key, probabilityData);
				//				}
				//				Text error = new Text("Error - Message");
			}
			context.write(key, new ProbabilityData("$","$",Step4.kFromArgs,k));
		}
	}

	public static class PartitionerClass extends Partitioner<Text, ProbabilityData>
	{
		final int DECADE = 12;
		@Override
		public int getPartition(Text key, ProbabilityData value, int numPartitions)
		{
			int year = value.getProbYear();
			int decade = year / 10;

			return decade % DECADE;
		}
	}

	public static void main(String[] args) throws Exception
	{	
		System.out.println("Start Step 4");
		Step4.kFromArgs = new Integer(args[2]);

		System.out.println("k recieved: " + kFromArgs);

		Configuration conf = new Configuration();
		Job job = new Job(conf, "step4");
		job.setJarByClass(Step4.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ProbabilityData.class);
		job.setNumReduceTasks(12);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}