package worker;

import java.util.List;

import com.amazonaws.services.sqs.model.Message;
import com.sun.mail.handlers.message_rfc822;
import com.sun.mail.imap.protocol.MessageSet;

public class Worker
{
	public static void main(String[] args)
	{
		// Initialization:
		  AmazonServices services = new AmazonServices();
		  while(true)
		  {
			  List<Message> messages = services.receiveMessages(services.managerWorkerQueueUrl);
			  for(Message msg : messages)
			  {
				  System.out.println(msg.getBody());
				  services.deleteMessages(services.managerWorkerQueueUrl, msg);
			  }
			  
		  }
	}
	
}
