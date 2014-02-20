package step3;

import java.io.IOException;

import models.ProbabilityValue;
import models.ValueTupple;
import models.WordPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Step3
{
	
	public static class MapClass extends Mapper<WordPair,ValueTupple, Text, ProbabilityValue>
	{
		@Override
		public void map(WordPair key, ValueTupple value, Context context) throws IOException,  InterruptedException
		{
			  //get information from old value
			  WordPair oldWordPair = value.getWordPair();
			  double oldCountWord1 = value.getCountWord1().get();			  
			  double oldCountWord2 = value.getCountWord2().get();
			  double oldCountWordPair = value.getCountWordPair();
			  double oldN = value.getN().get();
			  IntWritable oldYear = new IntWritable(value.getYear());
			  
			  //handle joint probability
			  double jointVal = oldCountWordPair / oldN;
			  DoubleWritable newJointVal = new DoubleWritable(jointVal);
			  Text joint = new Text("joint");
			  
			  //handle dice probability
			  double diceVal = (2*oldCountWordPair) / (oldCountWord1+oldCountWord1);
			  DoubleWritable newDiceVal = new DoubleWritable(diceVal);			  
			  Text dice = new Text("dice");
			  
			  //handle geometric probability
			  double geometricVal = Math.sqrt(jointVal*diceVal);
			  DoubleWritable newGeometricVal = new DoubleWritable(geometricVal);			  
			  Text geometric = new Text("geometric");			  
			  
			  //init newValues
			  ProbabilityValue newJointProbValue = new ProbabilityValue(oldWordPair, newJointVal, oldYear);
			  ProbabilityValue newDiceProbValue = new ProbabilityValue(oldWordPair, newDiceVal, oldYear);
			  ProbabilityValue newGeometricProbValue = new ProbabilityValue(oldWordPair, newGeometricVal, oldYear);
			  
			  //add probValues to context
			  context.write(joint , newJointProbValue);
			  context.write(dice , newDiceProbValue);
			  context.write(geometric , newGeometricProbValue);
		}
	}

	public static class ReduceClass extends Reducer<Text,ProbabilityValue,Text,ProbabilityValue>
	{	
		@Override
		public void reduce(Text key, Iterable<ProbabilityValue> values, Context context) throws IOException,  InterruptedException
		{
			
		}
	}

	public static class PartitionerClass extends Partitioner<Text, ProbabilityValue>
	{
		final int DECADE = 12;
		@Override
		public int getPartition(Text key, ProbabilityValue value, int numPartitions)
		{
			int year = value.getProbYear();
			int decade = year / 10;
			
			return decade % DECADE;
		}
	}

	public static void main(String[] args) throws Exception
	{	
		int kFromArgs = Integer.parseInt(args[2]);
		Configuration conf = new Configuration();
//		conf.set("mapred.map.tasks","10");
//		conf.set("mapred.reduce.tasks","11");
		Job job = new Job(conf, "step3");
		job.setJarByClass(Step3.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(36);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}


//int probabilityType = -1;
//String keyName = key.toString();
//switch (keyName)
//{
//case "joint":
//	probabilityType = 0;
//	break;
//case "dice":
//	probabilityType = 12;
//	break;
//case "geometric":
//	probabilityType = 24;
//	break;
//default:
//	break;
//}