package assignment2;

import java.io.IOException;
import java.util.ArrayList;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class AmazonServices
{
	  public AWSCredentials credentials;
	  AmazonElasticMapReduce mapReduce;
	  ArrayList<HadoopJarStepConfig> hadoopJarSteps;
	  ArrayList<StepConfig> stepsConfig;
	  JobFlowInstancesConfig instances;

	  public String bucketName;
	  
	  String jobFlowId;
	  	  
	  public AmazonServices()
	  {
		  this.stepsConfig = new ArrayList<>();
		  
		  try 
		  {
			  this.credentials = new PropertiesCredentials(AmazonServices.class.getResourceAsStream("../AwsCredentials.properties"));
		  }
		  catch (IOException e) 
		  {
			  e.printStackTrace();
		  } 
		  
		  mapReduce = new AmazonElasticMapReduceClient(credentials);
		   
		  HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
		      .withJar("s3n://akiajzfcy5fifmsaagrq/step1.jar") // This should be a full map reduce application.
		      .withMainClass("step1.Step1")
		      .withArgs("s3n://akiajzfcy5fifmsaagrq/input/input.txt", "s3n://akiajzfcy5fifmsaagrq/Step1/output");
		  //s3n://dsp112/eng.corp.10k
		  
//		  hadoopJarSteps.add(new HadoopJarStepConfig()
//	      .withJar("s3n://akiajzfcy5fifmsaagrq/Step2.jar") // This should be a full map reduce application.
//	      .withMainClass("some.pack.MainClass")
//	      .withArgs("s3n://akiajzfcy5fifmsaagrq/Step1/", "s3n://akiajzfcy5fifmsaagrq/Step2/"));
//		  
//		  hadoopJarSteps.add(new HadoopJarStepConfig()
//	      .withJar("s3n://akiajzfcy5fifmsaagrq/Step3.jar") // This should be a full map reduce application.
//	      .withMainClass("some.pack.MainClass")
//	      .withArgs("s3n://akiajzfcy5fifmsaagrq/Step2/", args[1] , args[2]));
		   
		  StepConfig stepConfig = new StepConfig()
		      .withName("step1")
		      .withHadoopJarStep(hadoopJarStep)
		      .withActionOnFailure("TERMINATE_JOB_FLOW");
		  
//		  stepsConfig.add(new StepConfig()
//	      .withName("step2")
//	      .withHadoopJarStep(hadoopJarSteps)
//	      .withActionOnFailure("TERMINATE_JOB_FLOW"));
//		  
//		  stepsConfig.add(new StepConfig()
//	      .withName("step2")
//	      .withHadoopJarStep(hadoopJarSteps)
//	      .withActionOnFailure("TERMINATE_JOB_FLOW"));
		  
		  instances = new JobFlowInstancesConfig()
		      .withInstanceCount(2)
		      .withMasterInstanceType(InstanceType.M1Small.toString())
		      .withSlaveInstanceType(InstanceType.M1Small.toString())
		      .withHadoopVersion("2.2.0").withEc2KeyName("manager")
		      .withKeepJobFlowAliveWhenNoSteps(false)
		      .withPlacement(new PlacementType("us-east-1a"));
		   
		  RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		      .withName("Assignmet2")
		      .withInstances(instances)
		      .withSteps(stepConfig)
		      .withLogUri("s3n://akiajzfcy5fifmsaagrq/logs/");
		   
		  RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		  this.jobFlowId = runJobFlowResult.getJobFlowId();
	  }
		  
	  public static void main(String [] args)
	  {
		  AmazonServices services = new AmazonServices();
		  System.out.println("Ran job flow with id: " + services.jobFlowId);
	  }
}

