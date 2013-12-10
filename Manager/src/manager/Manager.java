package manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.LinkedList;
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
	    	  for (Message message : services.receiveMessages(services.localManagerQueueUrl)) 
	    	  {
	    		  // parse message ( local application id | n | image url key | output file name ) 
	    		  String[] tokens = services.parseMessage(message.getBody());
	    		  
	    		  String loacalApplicationId = tokens[0];
	    		  int n = Integer.parseInt(tokens[1]);
	    		  String imageUrlListKey = tokens[2];
	    		  String outputFileKey = tokens[3];
	    		  
	    		  // create Task from imformation
	    		  Task task = new Task(loacalApplicationId,n , imageUrlListKey, outputFileKey);
	    		  
	    		  // delete message
	    		  services.deleteMessages(services.localManagerQueueUrl, message);
	    		  
	    		  // download imageList file from S3
	    		  File imageUrlListfile = services.downloadFile(imageUrlListKey, "imageUrlList.txt");
	    		  
				  // Count number of urls from the imageList file
	    		  LinkedList<String> oldUrls = getUrlsFromFile(imageUrlListfile);
	    		  int numOfUrls = oldUrls.size();
	    		  task.setOldUrls(oldUrls);
	    		  task.setNumberOfImages(numOfUrls);
	    		  
	    		  int numOfWorkers = numOfUrls/n;
	    		  
	    		  // For each url create massage and send to managerWorkerQueue
	    		  createTasksForWorkers(services, task);
	    		  
	    		  System.out.println("oldUrls: " + oldUrls.size());
	    		  
	    		  // create a request for workers and initialize
	    		  List<Instance> instances = initializeWorkers(services, numOfWorkers);
	    		  
	    		  // check if recieved back a message from workerManagerQueue add to resultList
	    		  task.getOldUrls().clear();
	    		  recieveCompletedTasks(services,task);
	    		  
	    		  System.out.println("oldUrls's size: " + task.getOldUrls().size());
	    		  System.out.println("newUrls's sise: " + task.getNewUrls().size());
	  	        
	    		  // Create HTML file from images
	    		  File outputfile = HtmlHandler.createHtmlFile(oldUrls, task.getNewUrls(), outputFileKey);

	    		  // Upload HTML file to S3
	    		  services.uploadFile(outputFileKey, outputfile);
	    		  System.out.println("HTML file was uploaded");
	  	        
	    		  // send message to localManagerQueue
	    		  services.sendMessage(services.managerLocalQueueUrl, loacalApplicationId + "\t" + outputFileKey);
	    		  
	    		  System.out.println("Done");
	    	  }
	      }
	    }
	    
	    // counts the number of URLs from file
	    public static LinkedList<String> getUrlsFromFile(File file)
	    {
	    	LinkedList<String> urls = new LinkedList<String>();
			try
			{
				String line;
				BufferedReader br = new BufferedReader(new FileReader(file));
				while ((line = br.readLine()) != null)
				{
					if(line.contains("http://"))
					{
						urls.add(line);
					}
				}
				br.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			return urls;
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

	    public static void createTasksForWorkers(AmazonServices services, Task task)
	    {
	    	while(!task.getOldUrls().isEmpty())
	    	{	
	    		String url = task.getOldUrls().pop();
	    		services.sendMessage(services.managerWorkerQueueUrl, url);
	    	}
	    }

	    public static void recieveCompletedTasks(AmazonServices services, Task task)
	    {
	    	System.out.println("NumOfImages: " + task.getNumberOfImages());
	    	System.out.println("NumOfImagesStamped: " + task.getNumberOfImagesStamped());
	    	System.out.println("isReady: " + task.isReady());
	    	
	    	while(!task.isReady())
	    	{
	    		for(Message msg : services.receiveMessages(services.workerManagerQueueUrl))
	    		{
	    			String[] tokens = services.parseMessage(msg.getBody());
	    			services.deleteMessages(services.workerManagerQueueUrl, msg);

	    			String oldUrl = tokens[0];
	    			String newUrl = tokens[1];

	    			// update task
	    			task.getOldUrls().add(oldUrl);
	    			task.getNewUrls().add(newUrl);
	    			task.incNumberOfImagesStamped();
	    		}
	    	}
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
}
