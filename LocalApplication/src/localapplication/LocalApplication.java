package localapplication;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;

public class LocalApplication
{
	public static void main(String[] args)
	{
		
		try
		{
			// Init services
			AmazonServicesLocal services = new AmazonServicesLocal();
			
			// creating the s3 bucket
			services.createBucket(services.bucketName);
			
			// create Local - Manager queues
			services.initQueues();
            
            // upload file image-urls.txt to S3
			String imageUrlKey = "imageUrlTxt";
            File imageFile = new File("../image-urls.txt");
            services.uploadFile(imageUrlKey, imageFile);
			System.out.println("file " + imageFile.getName() + " was uploaded successfually \n");

			// wait for user
			System.out.println("Now you can upload files Manually,\nwhen done, Press enter to continue...");
			System.in.read();
			
			// checks if a manager instance already exists
//			if(!isManagerRunning(services.ec2))
//			{
//				System.out.println("Inizializing new manager");
//				Instance manager = initializeManager(services);
//			}
	        
	        // send a test message
	        services.sendMessage(services.localManagerQueueUrl, "6\t"+imageUrlKey);
	        
	        // receive message
//	        Message message = services.receiveMessages(services.managerLocalQueueUrl).get(0);
			
			// download the HTML file from S3
//			services.downloadFile("resultHtmlFile", "result.html");

	        System.out.println("Done");
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
	
	public static Instance initializeManager(AmazonServicesLocal services)
	{
		// create a request for a computer 
		RunInstancesRequest request = new RunInstancesRequest();
		request.setImageId("ami-51792c38"); // supports java ami-598caf30  ami-c35674aa 
		request.setInstanceType(InstanceType.T1Micro.toString());
		request.setMinCount(1);
		request.setMaxCount(1);
		request.withKeyName("manager");
		request.withSecurityGroups("default");
		request.withUserData(services.getScript());
		
		// start instance
		return services.ec2.runInstances(request).getReservation().getInstances().get(0);
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
