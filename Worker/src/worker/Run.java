package worker;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;


public class Run
{

	public static void main(String[] args)
	{
		
		try
		{
			AWSCredentials credentials = new PropertiesCredentials(Run.class.getResourceAsStream("../AwsCredentials.properties"));

			AmazonS3 s3 = new AmazonS3Client(credentials);
			
	        
			PutObjectRequest request = new PutObjectRequest("akiajzfcy5fifmsaagrq", "testFileTxt", new File("../try.txt"));
			
			s3.putObject(request);
			
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
        // Receive messages
        
        
		// TODO Check MWAssQ
		
		// TODO Download the image file from url
		
		// TODO Add text to image
		
		// TODO Upload result to S3
		
		// TODO add message to MWResQ
		
		// TODO remove message if already in MWAssQ 
	}

}
