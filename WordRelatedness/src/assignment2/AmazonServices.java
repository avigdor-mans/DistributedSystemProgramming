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
	  JobFlowInstancesConfig instances;
	  ArrayList<StepConfig> steps;
	  HadoopJarStepConfig hadoopJarStep;
	  StepConfig stepConfig;

	  public String bucketName;
	  
	  String jobFlowId;
	  	  
	  public AmazonServices()
	  {
		  this.steps = new ArrayList<>();
		  
		  try 
		  {
			  this.credentials = new PropertiesCredentials(AmazonServices.class.getResourceAsStream("../AwsCredentials.properties"));
		  }
		  catch (IOException e) 
		  {
			  e.printStackTrace();
		  } 
		  
		  mapReduce = new AmazonElasticMapReduceClient(credentials);
		   
//		  HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
//		      .withJar("s3n://akiajzfcy5fifmsaagrq/step1.jar") // This should be a full map reduce application.
//		      .withMainClass("step1.Step1")
//		      .withArgs("s3n://akiajzfcy5fifmsaagrq/input/input.txt", "s3n://akiajzfcy5fifmsaagrq/Step1/output");
//		  
//		  stepConfig = new StepConfig()
//	      .withName("step1")
//	      .withHadoopJarStep(hadoopJarStep)
//	      .withActionOnFailure("TERMINATE_JOB_FLOW");
//		  //s3n://dsp112/eng.corp.10k
//		  
//		  steps.add(stepConfig);
//				  
//		  hadoopJarStep =  new HadoopJarStepConfig()
//	      .withJar("s3n://akiajzfcy5fifmsaagrq/step2.jar") // This should be a full map reduce application.
//	      .withMainClass("step2.Step2")
//	      .withArgs("s3n://akiajzfcy5fifmsaagrq/Step1/output/", "s3n://akiajzfcy5fifmsaagrq/Step2/output");
//		  
//		  stepConfig = new StepConfig()
//	      .withName("step2")
//	      .withHadoopJarStep(hadoopJarStep)
//	      .withActionOnFailure("TERMINATE_JOB_FLOW");
//		  
//		  steps.add(stepConfig);
//		  
//		  hadoopJarStep = new HadoopJarStepConfig()
//	      .withJar("s3n://akiajzfcy5fifmsaagrq/step3.jar") // This should be a full map reduce application.
//	      .withMainClass("step3.Step3")
//	      .withArgs("s3n://akiajzfcy5fifmsaagrq/Step2/output/", "s3n://akiajzfcy5fifmsaagrq/Step3/output");
//		  
//		  stepConfig = new StepConfig()
//	      .withName("step3")
//	      .withHadoopJarStep(hadoopJarStep)
//	      .withActionOnFailure("TERMINATE_JOB_FLOW");
//		  
//		  steps.add(stepConfig);
		  
		  hadoopJarStep = new HadoopJarStepConfig()
	      .withJar("s3n://akiajzfcy5fifmsaagrq/step4.jar") // This should be a full map reduce application.
	      .withMainClass("step4.Step4")
	      .withArgs("s3n://akiajzfcy5fifmsaagrq/Step3/output/", "s3n://akiajzfcy5fifmsaagrq/Step4/output", "10");
		  
		  stepConfig = new StepConfig()
	      .withName("step4")
	      .withHadoopJarStep(hadoopJarStep)
	      .withActionOnFailure("TERMINATE_JOB_FLOW");
		  
		  steps.add(stepConfig);
		  
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
		      .withSteps(steps)
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

