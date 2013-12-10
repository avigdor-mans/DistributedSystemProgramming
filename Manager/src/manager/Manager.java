package manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;

public class Manager
{
	  public static void main(String[] args) throws Exception
	  {
		  // Initialization:
		  AmazonServices services = new AmazonServices();
		  
		  // create queues for workers
		  CreateQueueRequest createQueueRequest = new CreateQueueRequest("managerWorkerQueue_akiajzfcy5fifmsaagrq");
	      services.sqs.createQueue(createQueueRequest).getQueueUrl();
	      
	      CreateQueueRequest createQueueRequest1 = new CreateQueueRequest("workerManagerQueue_akiajzfcy5fifmsaagrq");
	      services.sqs.createQueue(createQueueRequest1).getQueueUrl();
	      
		  // Check if there a message in imageList SQS and execute it
	      while(true)
	      {
	    	  // receive all messages from localManagerQueueUrl
	    	  List<Message> messages = services.receiveMessages(services.localManagerQueueUrl);
	    	  
	    	  for (Message message : messages) 
	    	  {
	    		  // parse message (local application id? | n | key ) 
	    		  String[] tokens = services.parseMessage(message.getBody());
	    		  
	    		  int n = Integer.parseInt(tokens[0]);
	    		  String imageTxtKey = tokens[1];
	    		  
	    		  // delete message
	    		  services.deleteMessages(services.localManagerQueueUrl, message);
	    		  
	    		  services.sendMessage(services.managerWorkerQueueUrl, imageTxtKey);
	    		  services.sendMessage(services.managerWorkerQueueUrl, "batz");
	    		  services.sendMessage(services.managerWorkerQueueUrl, "hello");
	    		  
	    		  
	    		  // download imageList file from S3
	    		  File imageUrlListfile = services.downloadFile(imageTxtKey, "imageUrlList.txt");
	    		  
				  // Count number of urls from the imageList file
	    		  int numOfUrls = countNumOfUrlsFromFile(imageUrlListfile);
	    		  
	    		  File tempFile = createTemporaryFile("" + numOfUrls);
	    		  
	    		  services.uploadFile("testFileTxt", tempFile);
	    		  
	    		  int numOfWorkers = numOfUrls/n;
	    		  
	    		  // TODO For each url create massage and send to managerWorkerQueue
	    		  
	  	        
	    		  // create a request for workers and initialize
//	    		  List<Instance> instances = initializeWorkers(ec2, numOfWorkers);
	    		  
	    		  // TODO check if recieved back a message from workerManagerQueue add to returnedMessagesList
	  	        
	    		  // TODO Create HTML file from images

	    		  // TODO Upload HTML file to S3
	  	        
	    		  // TODO send message to localManagerQueue
	    		  
	    	  }

	      }
		  
		  //sendMessage(sqs, managerWorkerQueueUrl, str);
	        
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
	    
	    public static List<Instance> initializeWorkers(AmazonServices services, int numOfWorkers)
	    {
			RunInstancesRequest request = new RunInstancesRequest();
			request.setImageId("ami-51792c38"); // supports java ami-598caf30  ami-c35674aa 
			request.setInstanceType(InstanceType.T1Micro.toString());
			request.setMinCount(numOfWorkers);
			request.setMaxCount(numOfWorkers);
			request.withKeyName("Worker_" + UUID.randomUUID());
			request.withSecurityGroups("default");
			request.withUserData(services.getScript());
			
			// start instance
			List<Instance> instances = services.ec2.runInstances(request).getReservation().getInstances();
			return instances;
	    }


}
