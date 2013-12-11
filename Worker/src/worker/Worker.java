package worker;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.amazonaws.services.sqs.model.Message;

public class Worker
{
	public static void main(String[] args)
	{
		// Initialization:
		  AmazonServices services = new AmazonServices();
		  while(true)
		  {
			  // Receive messages
			  for(Message msg : services.receiveMessages(services.managerWorkerQueueUrl))
			  {
				  // Parse message ( url )
				  String[] tokens = services.parseMessage(msg.getBody());
				  String oldUrl = tokens[0];

				  // remove message
				  System.out.println("deleting message: "+ msg.getBody() + " from queue");
				  services.deleteMessages(services.managerWorkerQueueUrl, msg);
					
				  // Download the image file from url and add text to image
				  String fileNamekey = UUID.randomUUID()+".png";
				  File file = null;
				  
				  try
				  {
					  file = ImageHandler.createStamp(oldUrl, "tmpImage.png");
				  }
				  catch (IOException e)
				  {
					  e.printStackTrace();
				  }
				  
				  System.out.println("stamp for image: "+ fileNamekey + " was created");
				  
				  // Upload result to S3
				  services.uploadFile(fileNamekey, file);
				  
				  String newUrl = "https://s3.amazonaws.com/akiajzfcy5fifmsaagrq/" + fileNamekey;
				  // add message to workerManagerQueue	( old url | new url )
				  services.sendMessage(services.workerManagerQueueUrl, oldUrl + "\t" + newUrl);
				  
				  System.out.println("image: " + fileNamekey + " was created");
			  }
			  
		  }
	}
	
}
