package manager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

public class Manager
{
	  public static void main(String[] args) throws Exception
	  {
		  /// TODO Check if there a message in imageList SQS
		  
		  /// TODO download imageList from S3
		  
		  /// Count number of urls
		  try
		  {
			int count = 0;
			String line;
			BufferedReader br = new BufferedReader(new FileReader("image-urls.txt"));
			while ((line = br.readLine()) != null)
			{
				if(line.contains("http://"))
				{
					count++;
				}
			}
			
			System.out.println(count);
			}
		  catch (IOException e)
		  {
				e.printStackTrace();
		  }
		  
		  AWSCredentials credentials = new PropertiesCredentials(Manager.class.getResourceAsStream("../AwsCredentials.properties"));
		  // create sqs for workers
		  AmazonSQS sqs = new AmazonSQSClient(credentials);
        		   
//	      CreateQueueRequest createQueueRequest = new CreateQueueRequest("MWAssQ1");
//	      String MWAssQ = sqs.createQueue(createQueueRequest).getQueueUrl();
//	        
//	      CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("MWResQ1");
//	      String MWResQ = sqs.createQueue(createQueueRequest1).getQueueUrl();

	      /// Create number Worker instances
	      AmazonEC2 worker = new AmazonEC2Client(credentials);
	        try
	        {
	            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
	            RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", 1, 1); // TODO change number of Workers
	            request.setInstanceType(InstanceType.T1Micro.toString());
	            List<Instance> instances = worker.runInstances(request).getReservation().getInstances();
	            System.out.println("Launch instances: " + instances);
	        }
	        catch (AmazonServiceException ase)
	        {
	            System.out.println("Caught Exception: " + ase.getMessage());
	            System.out.println("Reponse Status Code: " + ase.getStatusCode());
	            System.out.println("Error Code: " + ase.getErrorCode());
	            System.out.println("Request ID: " + ase.getRequestId());
	        }
	        
	        /// TODO For each url create massage to  
	        
	        /// TODO check if recieved Count messages from MWResQ
	        
	        /// TODO Create HTML file from images
	        
	        /// TODO Upload to S3
	        
	        /// TODO send message to LMResQ
	        
	    }

}
