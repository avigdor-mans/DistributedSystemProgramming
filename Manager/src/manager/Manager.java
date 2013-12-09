package manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Manager
{
	  public static void main(String[] args) throws Exception
	  {
		  // hard coded things
		  String bucketName = "akiajzfcy5fifmsaagrq";
		  String localManagerQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/localManagerQueue_akiajzfcy5fifmsaagrq";
		  String managerLocalQueueUrl = "https://sqs.us-east-1.amazonaws.com/677422349073/managerLocalQueue_akiajzfcy5fifmsaagrq";

		  // Initialization:
		  AWSCredentials credentials = new BasicAWSCredentials("AKIAJZFCY5FIFMSAAGRQ","JHAB/lX5xrjOu+Vj6b294f0hpxF7oqJt8UGAItbo");
		  
		  // connect to the S3 service
		  AmazonS3 s3 = new AmazonS3Client(credentials);
		   
		  // connect to SQS service
		  AmazonSQS sqs = new AmazonSQSClient(credentials);
		  
		  // connect to EC2 service
		  AmazonEC2 ec2 = new AmazonEC2Client(credentials);
		  
		  // create sqs for workers
	      CreateQueueRequest createQueueRequest = new CreateQueueRequest("managerWorkerQueue");
	      String managerWorkerQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
	      CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("workerManagerQueue");
	      String workerManagerQueueUrl = sqs.createQueue(createQueueRequest1).getQueueUrl();
	      
	      
		  // Check if there a message in imageList SQS and execute it
	      while(true)
	      {
	    	  // receive all messages from localManagerQueueUrl
	    	  List<Message> messages = receiveMessages(sqs, localManagerQueueUrl);
	    	  
	    	  for (Message message : messages) 
	    	  {
	    		  // parse message ( n | key ) 
	    		  String[] tokens = parseMessage(message.getBody());
	    		  
	    		  int n = Integer.parseInt(tokens[0]);
	    		  String imageTxtKey = tokens[1];

	    		  deleteMessages(sqs, localManagerQueueUrl, message);
	    		  
	    		  // download imageList file from S3
	    		  File imageUrlListfile = downloadFile(s3, bucketName, imageTxtKey, "imageUrlList.txt");
	    		  
				  // Count number of urls from the imageList file
	    		  int numOfUrls = countNumOfUrlsFromFile(imageUrlListfile);
	    		  
	    		  createTemporaryFile("" + numOfUrls);
	    		  
	    		  int numOfWorkers = numOfUrls/n;
	    		  
	  	        // TODO For each url create massage and send to sqs  
	  	        
	    		// create a request for workers and initialize
//	    		List<Instance> instances = initializeWorkers(ec2, numOfWorkers);
	    		  
		  	    // TODO Create HTML file from images
	    		  
	  	        // TODO check if recieved back a message from workerManagerQueue ad add to html
	  	        
	    		// TODO Add Last Lines to HTML
	  	        
	  	        // TODO Upload to S3
//	    		uploadFile(s3, bucketName, "testFileTxt", createTemporaryFile("hello"));
	  	        
	  	        // TODO send message to LMResQ
	    		  
	    	  }

	      }
		  
		  //sendMessage(sqs, managerWorkerQueueUrl, str);
	        
	    }
	  
	    // send message to a specific queue
	    public static void sendMessage(AmazonSQS sqs, String queueUrl , String message)
	    {
	        sqs.sendMessage(new SendMessageRequest(queueUrl, message));
	    }
	  
		// receive message from a specific
	    public static List<Message> receiveMessages(AmazonSQS sqs, String QueueUrl)
	    {
	        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QueueUrl);
	        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	        
	        return messages; 	//	message.getBody()
	    }
	    
	    // delete message from queue 
	    public static void deleteMessages(AmazonSQS sqs, String QueueUrl, Message message)
	    {
	    	String messageRecieptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(QueueUrl, messageRecieptHandle));
	    }
	    
	    // parse message
	    public static String[] parseMessage(String msg)
	    {
		    String[] tokens = msg.split("\t");
		    return tokens;
	    }
	    
		// create temporary file
	    public static File createTemporaryFile(String content)
	    {
			File file = null;
			try
			{
				file = File.createTempFile("testFile", ".txt");
				Writer writer = new OutputStreamWriter(new FileOutputStream(file));
				writer.write(content);
				writer.close();
			} catch (IOException e)
			{			
				e.printStackTrace();
			}
			return file;
	    }
	    
	    /**
	     * upload file to S3
	     * @param s3 - pointer to the S3 service
	     * @param bucketName - the requested bucketName
	     * @param file - the file wished to upload
	     */
	    public static void uploadFile(AmazonS3 s3, String bucketName, String keyName, File file)
	    {
			PutObjectRequest request = new PutObjectRequest(bucketName, keyName, file);
			s3.putObject(request);
	    }
	    
	    /**
	     * download file from S3 and saves on PC
	     * @param s3
	     * @param bucketName
	     * @param keyName
	     * @param localFilePath
	     * @return File objects
	     */
	    public static File downloadFile(AmazonS3 s3, String bucketName, String keyName , String localFilePath)
	    {
	    	File localFile = new File(localFilePath);
	    	
	    	ObjectMetadata object = s3.getObject(new GetObjectRequest(bucketName, keyName), localFile);
	    	
	    	return localFile;
	    }
	    
	    // counts the number of URLs from file
	    public static int countNumOfUrlsFromFile(File file)
	    {
	    	int count = 0;
			try
			{
				String line;
				BufferedReader br = new BufferedReader(new FileReader(file));
				while ((line = br.readLine()) != null)
				{
					if(line.contains("http://"))
					{
						count++;
					}
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			return count;
	    }
	    
		public static String getScript()
		{
			ArrayList<String> lines = new ArrayList<String>();
			lines.add("#! /bin/bash");
			lines.add("wget https://s3.amazonaws.com/akiajzfcy5fifmsaagrq/worker.jar");
			lines.add("java -jar /worker.jar");
			String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
			return str;
		}
	    
		// joins all lines of script to one
	    static String join(Collection<String> s, String delimiter) 
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
	    
	    public static List<Instance> initializeWorkers(AmazonEC2 ec2, int numOfWorkers)
	    {
			RunInstancesRequest request = new RunInstancesRequest();
			request.setImageId("ami-51792c38"); // supports java ami-598caf30  ami-c35674aa 
			request.setInstanceType(InstanceType.T1Micro.toString());
			request.setMinCount(numOfWorkers);
			request.setMaxCount(numOfWorkers);
			request.withKeyName("Worker_" + UUID.randomUUID());
			request.withSecurityGroups("default");
			request.withUserData(getScript());
			
			// start instance
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			return instances;
	    }


}
