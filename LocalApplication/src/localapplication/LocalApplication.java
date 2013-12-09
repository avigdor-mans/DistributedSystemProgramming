package localapplication;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class LocalApplication
{
	public static void main(String[] args)
	{
		
		try
		{
			// load credentials
			AWSCredentials credentials = new PropertiesCredentials(LocalApplication.class.getResourceAsStream("../AwsCredentials.properties"));
			
			// create SQS Service
			AmazonSQS sqs = new AmazonSQSClient(credentials);
			
			// Add queues
	        System.out.println("Creating a new SQS queue called localManagerQueue.\n");
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest("localManagerQueue_"+credentials.getAWSAccessKeyId().toLowerCase());
	        String localManagerQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	        System.out.println("address: " + localManagerQueueUrl);
	        
	        System.out.println("Creating a new SQS queue called managerLocalQueue.\n");
	        createQueueRequest = new CreateQueueRequest("managerLocalQueue_"+credentials.getAWSAccessKeyId().toLowerCase());
	        String managerLocalQueueURL = sqs.createQueue(createQueueRequest).getQueueUrl();
	        System.out.println("address: " + localManagerQueueUrl);
	        
			// create a S3 Service
			AmazonS3 s3 = new AmazonS3Client(credentials);
			
			String bucketName =  credentials.getAWSAccessKeyId().toLowerCase();
			String imageUrlKey = "imageUrlTxt";
			
			// creating the s3 bucket
			System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);
            
            // upload file image-urls.txt to S3
//            File imageFile = new File("../image-urls.txt");
//			PutObjectRequest s3Request = new PutObjectRequest(bucketName, imageUrlKey, imageFile);
//			s3.putObject(s3Request);
//			System.out.println("file " + imageFile.getName() + " was uploaded successfually \n");
			
			System.out.println("Now you can upload files Manually,\n when done, Press enter to continue");
			System.in.read();
			
			// creating the ec2 Service
			AmazonEC2 ec2 = new AmazonEC2Client(credentials);
			
			// checks if a manager instance already exists
			if(!isManagerRunning(ec2))
			{
				System.out.println("Inizializing new manager");
				Instance manager = initializeManager(ec2);
			}
	        
	        // send a test message
	        sendMessage(sqs, LMQueueUrl, "batz");	
			
			// TODO download the html file from S3
			
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
		catch (AmazonServiceException ase)
		{
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
		
		
	}
	public static Instance initializeManager(AmazonEC2 ec2)
	{
		// create a request for a computer 
		RunInstancesRequest request = new RunInstancesRequest();
		request.setImageId("ami-51792c38"); // supports java ami-598caf30  ami-c35674aa 
		request.setInstanceType(InstanceType.T1Micro.toString());
		request.setMinCount(1);
		request.setMaxCount(1);
		request.withKeyName("manager");
		request.withSecurityGroups("default");
		request.withUserData(getScript());
		
		// start instance
		return ec2.runInstances(request).getReservation().getInstances().get(0);
	}
	
	// the script to add to the node's user-data
	public static String getScript()
	{
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash");
		lines.add("cd /");
		lines.add("wget https://s3.amazonaws.com/akiajzfcy5fifmsaagrq/manager.jar");
		lines.add("java -jar manager.jar");
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
    
    // send message with the imageUrlKey, n (number of URLs per worker)
    public static void sendMessage(AmazonSQS sqs,String url, String message)
    {
        System.out.println("Sending a message to LMQueue.\n");
        sqs.sendMessage(new SendMessageRequest(url, message)); //LMQueueUrl
    }
    
	// receive message from manager
    public static String receiveMessage(AmazonSQS sqs, String QueueUrl)
    {
        System.out.println("Receiving messages from LMQueue.\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QueueUrl);
        Message message = sqs.receiveMessage(receiveMessageRequest).getMessages().get(0);
        return message.getBody();
    }
    
    public static boolean isManagerRunning(AmazonEC2 ec2)
    {
    	boolean res = false;
    	
		List<Reservation> reservList = ec2.describeInstances().getReservations();
		if(!reservList.isEmpty())
		{
			for(Reservation reservation : reservList)
			{
				List<Instance> instances = reservation.getInstances();
				for(Instance instance: instances)
				{
					System.out.println("found instance name: " + instance.getKeyName() + " State: " + instance.getState().getName());
					if(instance.getKeyName() == "manager" && instance.getState().getName().equals("running"))
					{
						res = true;
					}
					else
					{
						res = false;
					}
				}
			}
		}
		
		return res;
    }

}
