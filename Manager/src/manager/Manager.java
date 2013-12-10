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
	    	  List<Message> messages = services.receiveMessages(services.localManagerQueueUrl);
	    	  String localApplicationId = null;
	    	  
	    	  for (Message message : messages) 
	    	  {
	    		  // parse message (local application id | n | key ) 
	    		  String[] tokens = services.parseMessage(message.getBody());
	    		  
	    		  String loacalApplicationName = tokens[0];
	    		  int n = Integer.parseInt(tokens[1]);
	    		  String imageTxtKey = tokens[2];
	    		  String outputPath = tokens[3];
	    		  
	    		  // delete message
	    		  services.deleteMessages(services.localManagerQueueUrl, message);
	    		  
	    		  // download imageList file from S3
	    		  File imageUrlListfile = services.downloadFile(imageTxtKey, "imageUrlList.txt");
	    		  
				  // Count number of urls from the imageList file
	    		  LinkedList<String> oldUrls = getUrlsFromFile(imageUrlListfile);
	    		  int numOfUrls = oldUrls.size();
	    		  
	    		  System.out.println("num of urls: " + numOfUrls);
	    		  
//	    		  File tempFile = createTemporaryFile("" + numOfUrls);
//	    		  services.uploadFile("testFileTxt", tempFile);
	    		  
	    		  int numOfWorkers = numOfUrls/n;
	    		  
	    		  System.out.println("num of workers: " + numOfWorkers);
	    		  
	    		  // For each url create massage and send to managerWorkerQueue
//	    		  createTasksForWorkers(services, oldUrls, loacalApplicationName);
	    		  
	    		  System.out.println("oldUrls: " + oldUrls.size());
	    		  
	    		  // create a request for workers and initialize
//	    		  List<Instance> instances = initializeWorkers(ec2, numOfWorkers);
	    		  
	    		  // check if recieved back a message from workerManagerQueue add to resultList
	    		  oldUrls.clear();
	    		  LinkedList<String> newUrls = new LinkedList<String>();
	    		  
	    		  localApplicationId = recieveCompletedTasks(services,oldUrls,newUrls);
	    		  
	    		  System.out.println("oldUrls: " + oldUrls.size());
	    		  System.out.println("oldUrls: " + oldUrls.size());
	  	        
	    		  // Create HTML file from images
	    		  File outputfile = HtmlHandler.createHtmlFile(oldUrls, newUrls, outputPath);

	    		  // Upload HTML file to S3
	    		  services.uploadFile(outputPath, outputfile);
	  	        
	    		  // send message to localManagerQueue
	    		  services.sendMessage(services.managerLocalQueueUrl, "done");
	    		  
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

	    public static void createTasksForWorkers(AmazonServices services, LinkedList<String> urls, String loacalApplicationName)
	    {
	    	while(!urls.isEmpty())
	    	{	
	    		String url = urls.pop();
	    		services.sendMessage(services.managerWorkerQueueUrl,
	    							 loacalApplicationName + "\t" + url);
	    	}
	    	
	    	
	    }

	    public static String recieveCompletedTasks(AmazonServices services, LinkedList<String> oldUrls, LinkedList<String> newUrls)
	    {
	    	String localAplicationId = null;

	    	List<Message> workersResultMessages = services.receiveMessages(services.workerManagerQueueUrl);
	    	for(Message msg : workersResultMessages)
	    	{
	    		String[] tokens = services.parseMessage(msg.getBody());
	    		localAplicationId = tokens[0];
	    		String oldUrl = tokens[1];
	    		String newUrl = tokens[2];
	    		
	    		oldUrls.add(oldUrl);
	    		newUrls.add(newUrl);
	    		
	    	}
	    	return localAplicationId;
	    }
}
