package localapplication;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AmazonServicesLocal
{
	  public AWSCredentials credentials;
	  public AmazonS3 s3;
	  public AmazonEC2 ec2;
	  public AmazonSQS sqs;
	  public String bucketName;
	  public String localManagerQueueUrl; 
	  public String managerLocalQueueUrl;
	  
	  public AmazonServicesLocal()
	  {
		  try 
		  {
			  this.credentials = new PropertiesCredentials(LocalApplication.class.getResourceAsStream("../AwsCredentials.properties"));
		  }
		  catch (IOException e) 
		  {
			  e.printStackTrace();
		  } 
		  
		  // connect to the S3 service
		  this.s3 = new AmazonS3Client(credentials);

		  // connect to EC2 service
		  this.ec2 = new AmazonEC2Client(credentials);
		  
		  // connect to SQS service
		  this.sqs = new AmazonSQSClient(credentials);
		  
	      // hard coded things
	      this.bucketName = credentials.getAWSAccessKeyId().toLowerCase();
	      
	      this.localManagerQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/localManagerQueue_akiajzfcy5fifmsaagrq";
	      
	      this.managerLocalQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/managerLocalQueue_akiajzfcy5fifmsaagrq";
	  }
	  
	  public void initQueues()
	  {
		// Add queues
	        System.out.println("Creating a new SQS queue called localManagerQueue.\n");
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest("localManagerQueue_"+credentials.getAWSAccessKeyId().toLowerCase());
	        this.sqs.createQueue(createQueueRequest).getQueueUrl();
	        System.out.println("address: " + localManagerQueueUrl);
	        
	        System.out.println("Creating a new SQS queue called managerLocalQueue.\n");
	        createQueueRequest = new CreateQueueRequest("managerLocalQueue_"+credentials.getAWSAccessKeyId().toLowerCase());
	        this.sqs.createQueue(createQueueRequest).getQueueUrl();
	        System.out.println("address: " + localManagerQueueUrl);
	  }
	  
	  // creating the s3 bucket
	  public void createBucket(String bucketName)
	  {
		  System.out.println("Creating bucket " + bucketName + "\n");
          this.s3.createBucket(bucketName);
	  }
	  
	  // send message to a specific queue
	  public void sendMessage(String queueUrl , String message)
	  {
		  this.sqs.sendMessage(new SendMessageRequest(queueUrl, message));
	  }
	  
	  // receive message from a specific
	  public List<Message> receiveMessages(String queueUrl)
	  {
		  ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		  List<Message> messages = this.sqs.receiveMessage(receiveMessageRequest).getMessages();
		  
		  return messages; 	//	message.getBody()
	  }
	   
	  // delete message from queue 
	  public void deleteMessages(String queueUrl, Message message)
	  {
		  String messageRecieptHandle = message.getReceiptHandle();
		  this.sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
	  }	   
	  
	  // parse message
	  public String[] parseMessage(String msg)
	  {
		  String[] tokens = msg.split("\t");
		  return tokens;
	  }

	  /**
	   * upload file to S3
	   * @param s3 - pointer to the S3 service
	   * @param bucketName - the requested bucketName
	   * @param file - the file wished to upload
	   */
	  public void uploadFile(String keyName, File file)
	  {
		  PutObjectRequest request = new PutObjectRequest(this.bucketName, keyName, file);
		  request.setCannedAcl(CannedAccessControlList.PublicRead);
		  this.s3.putObject(request);
	  }
	  
	  /**
	   * download file from S3 and saves on PC
	   * @param s3
	   * @param bucketName
	   * @param keyName
	   * @param localFilePath
	   * @return File objects
	   */
	  public File downloadFile(String keyName , String localFilePath)
	  {
		  File localFile = new File(localFilePath);
		
		  this.s3.getObject(new GetObjectRequest(this.bucketName, keyName), localFile);
		
		  return localFile;
	  }
	  
	  // bootstrap user-data
	  public String getScript()
	  {
		  ArrayList<String> lines = new ArrayList<String>();
		  lines.add("#! /bin/bash");
		  lines.add("cd /");
		  lines.add("touch imageUrlList.txt");
		  lines.add("wget https://s3.amazonaws.com/akiajzfcy5fifmsaagrq/manager.zip");
		  lines.add("unzip -P chenbardrorven manager.zip");
		  lines.add("java -jar manager.jar >& 1.log");
		  String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
		  return str;
	  }
	    
	  // joins all lines of script to one
	  public String join(Collection<String> s, String delimiter) 
	  {
		  StringBuilder builder = new StringBuilder();
		  Iterator<String> iter = s.iterator();
		  while (iter.hasNext())
		  {
			  builder.append(iter.next());
			  if (!iter.hasNext())
			  {
				  break;
			  }
			  builder.append(delimiter);
		  }
		  return builder.toString();
	  }
	  
}
