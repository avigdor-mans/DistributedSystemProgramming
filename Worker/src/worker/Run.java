package worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
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
			AWSCredentials credentials = new BasicAWSCredentials("AKIAJZFCY5FIFMSAAGRQ","JHAB/lX5xrjOu+Vj6b294f0hpxF7oqJt8UGAItbo");

			AmazonS3 s3 = new AmazonS3Client(credentials);

			// create temporary file
			File file = File.createTempFile("test", ".txt");
			Writer writer = new OutputStreamWriter(new FileOutputStream(file));
			writer.write("The quick brown fox jumps over lazy dog");
			writer.close();
			
			// bucket name, file's Key , file 
			PutObjectRequest request = new PutObjectRequest("akiajzfcy5fifmsaagrq", "testFileTxt", file);
			
			s3.putObject(request);
			
		}
		catch (IOException e)
		{
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
