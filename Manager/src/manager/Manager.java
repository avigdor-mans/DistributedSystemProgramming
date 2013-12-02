package manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class Manager
{
	  public static void main(String[] args) throws Exception
	  {
		  /// TODO Check if there a message in imageList SQS
		  
		  /// TODO download imageList from S3
		  
		  // Count number of urls
//		  try
//		  {
//			int count = 0;
//			String line;
//			BufferedReader br = new BufferedReader(new FileReader("image-urls.txt"));
//			while ((line = br.readLine()) != null)
//			{
//				if(line.contains("http://"))
//				{
//					count++;
//				}
//			}
//			
//			System.out.println(count);
//			}
//		  catch (IOException e)
//		  {
//				e.printStackTrace();
//		  }
		  
		  AWSCredentials credentials = new BasicAWSCredentials("AKIAJZFCY5FIFMSAAGRQ","JHAB/lX5xrjOu+Vj6b294f0hpxF7oqJt8UGAItbo");
		  
		  AmazonSQS sqs = new AmazonSQSClient(credentials);
		  
		  String message = receiveMessage(sqs, args[0]);
		  
		  AmazonS3 s3 = new AmazonS3Client(credentials);
		  
		  try
		  {
			// create temporary file
			File file = File.createTempFile("Hello", ".txt");
			Writer writer = new OutputStreamWriter(new FileOutputStream(file));
			writer.write(message);
			writer.close();
			
			// bucket name, file's Key , file 
			PutObjectRequest request = new PutObjectRequest("akiajzfcy5fifmsaagrq", "testFileTxt", file);
			
			s3.putObject(request);
		  }
		  catch (IOException e)
		  {
			e.printStackTrace();
		  }

		  // create sqs for workers
//	      CreateQueueRequest createQueueRequest = new CreateQueueRequest("MWAssQ1");
//	      String MWAssQ = sqs.createQueue(createQueueRequest).getQueueUrl();
//	        
//	      CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("MWResQ1");
//	      String MWResQ = sqs.createQueue(createQueueRequest1).getQueueUrl();

	      /// Create number Worker instances
//	      AmazonEC2 worker = new AmazonEC2Client(credentials);
//	        try
//	        {
//	            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
//	            RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1); // TODO change number of Workers
//	            request.setInstanceType(InstanceType.T1Micro.toString());
//	            List<Instance> instances = worker.runInstances(request).getReservation().getInstances();
//	            System.out.println("Launch instances: " + instances);
//	        }
//	        catch (AmazonServiceException ase)
//	        {
//	            System.out.println("Caught Exception: " + ase.getMessage());
//	            System.out.println("Reponse Status Code: " + ase.getStatusCode());
//	            System.out.println("Error Code: " + ase.getErrorCode());
//	            System.out.println("Request ID: " + ase.getRequestId());
//	        }

	        /// TODO For each url create massage to  
	        
	        /// TODO check if recieved Count messages from MWResQ
	        
	        /// TODO Create HTML file from images
	        
	        /// TODO Upload to S3
	        
	        /// TODO send message to LMResQ
	        
	    }
	  
		// receive message from manager
	    public static String receiveMessage(AmazonSQS sqs, String QueueUrl)
	    {
	        System.out.println("Receiving messages from LMQueue.\n");
	        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QueueUrl);
	        Message message = sqs.receiveMessage(receiveMessageRequest).getMessages().get(0);
	        return message.getBody();
	    }

}
