package worker;

import java.io.File;
import java.util.List;



import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AmazonServices
{
	  public AWSCredentials credentials;
	  public AmazonS3 s3;
	  public AmazonEC2 ec2;
	  public AmazonSQS sqs;
	  public String bucketName;
	  public String localManagerQueueUrl; 
	  public String managerLocalQueueUrl;
	  public String managerWorkerQueueUrl;
	  public String workerManagerQueueUrl;
	  
	  public AmazonServices()
	  {
		  this.credentials = new BasicAWSCredentials("AKIAJZFCY5FIFMSAAGRQ","JHAB/lX5xrjOu+Vj6b294f0hpxF7oqJt8UGAItbo"); 
		  
		  // connect to the S3 service
		  this.s3 = new AmazonS3Client(credentials);

		  // connect to EC2 service
		  this.ec2 = new AmazonEC2Client(credentials);
		  
		  // connect to SQS service
		  this.sqs = new AmazonSQSClient(credentials);
		  
	      // hard coded things
	      this.bucketName = "akiajzfcy5fifmsaagrq";
	      
	      this.localManagerQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/localManagerQueue_akiajzfcy5fifmsaagrq";
	      
	      this.managerLocalQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/managerLocalQueue_akiajzfcy5fifmsaagrq";
	      
	      this.managerWorkerQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/managerWorkerQueue_akiajzfcy5fifmsaagrq";
		  
	      this.workerManagerQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/workerManagerQueue_akiajzfcy5fifmsaagrq";
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
		  
		  return messages; 
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
	   * @param fileKeyName - the requested file ket name
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
	   * @param keyName
	   * @param localFilePath
	   * @return File objects
	   */
	  public File downloadFile(String keyName , String localFilePath)
	  {
		  File localFile = new File(localFilePath);
		  GetObjectRequest request = new GetObjectRequest(this.bucketName, keyName);
		  
		  this.s3.getObject(request, localFile);  // returns ObjectMetadata if needed
		
		  return localFile;
	  }
	  
}
