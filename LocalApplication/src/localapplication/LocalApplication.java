package localapplication;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.sqs.model.Message;

public class LocalApplication
{
	public static void main(String[] args)
	{
		
		try
		{
			// Init services
			AmazonServicesLocal services = new AmazonServicesLocal();
			String localApplicationId = UUID.randomUUID().toString();
			
			// creating the s3 bucket
			services.createBucket(services.bucketName);
			
			// create Local - Manager queues
			services.initQueues();
            
            // upload file image-urls.txt to S3
			String imageUrlListKey = "imageUrlList_" + UUID.randomUUID();

			File imageFile = new File(args[0]); 
            services.uploadFile(imageUrlListKey, imageFile);
			System.out.println("file " + imageFile.getName() + " was uploaded successfually \n");

			// wait for user
			System.out.println("Now you can upload files Manually,\nwhen done, Press enter to continue...");
			System.in.read();
			
			// checks if a manager instance already exists
			if(!isManagerRunning(services))
			{
				System.out.println("Inizializing new manager");
				initializeManager(services);  // Instance manager
			}
	        
	        // send a message to Manager 					   
			//( local application id | n | image url key | output file name )		    	args[1] = n							  args[2] = outputfile
	        services.sendMessage(services.localManagerQueueUrl, localApplicationId + "\t" + args[1] + "\t"+imageUrlListKey + "\t"+args[2]); 

	        // receive message
	        System.out.println("Wating for result, please wait...");
	        String outputFileKey = waitForAnswer(services, localApplicationId);
			
			// download the HTML file from S3
			services.downloadFile(outputFileKey, args[2]);

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
	
    public static boolean isManagerRunning(AmazonServicesLocal services)
    {
    	boolean res = false;
    	
		List<Reservation> reservList = services.ec2.describeInstances().getReservations();
		if(!reservList.isEmpty())
		{
			whenFound:
			for(Reservation reservation : reservList)
			{
				List<Instance> instances = reservation.getInstances();
				for(Instance instance: instances)
				{
					System.out.println("found instance name: " + instance.getKeyName() + " State: " + instance.getState().getName());
					if(instance.getKeyName() != null && instance.getKeyName().equals("manager") && instance.getState().getName().equals("running"))
					{
						res = true;
						break whenFound;
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
    
    public static String waitForAnswer(AmazonServicesLocal services,String localApplicationId)
    {
    	String res = null;
    	while(res == null)
    	{
    		for(Message msg : services.receiveMessages(services.managerLocalQueueUrl))
    		{
    			String[] tokens = services.parseMessage(msg.getBody());
    			if(localApplicationId.equals(tokens[0]))
    			{
    				res = tokens[1];
    		        services.deleteMessages(services.managerLocalQueueUrl, msg);
    			}
    		}
    	}
    	return res;
	}

}
