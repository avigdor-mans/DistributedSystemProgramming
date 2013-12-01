package localapplication;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class LocalApplication
{
	public static void main(String[] args)
	{
		
		try
		{
			// load credentials
			AWSCredentials credentials = new PropertiesCredentials(LocalApplication.class.getResourceAsStream("../AwsCredentials.properties"));
			AmazonS3 s3 = new AmazonS3Client(credentials);
			
			String bucketName =  credentials.getAWSAccessKeyId().toLowerCase();
			String imageUrlKey = "imageUrlTxt";
			
			// creating the s3 bucket
			System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);
            
            // upload file image-urls.txt to S3
            File imageUrl = new File("../image-urls.txt");
			PutObjectRequest s3Request = new PutObjectRequest(bucketName, imageUrlKey, imageUrl);
			s3.putObject(s3Request);
			System.out.println("file " + imageUrl.getName() + " was uploaded successfually \n");
			
			
			//wait to user authorization
			System.out.println("When uploading files to s3 is complete press enter to continue");
			System.in.read();
			
			
			// creating the ec2 manager
			AmazonEC2 manager = new AmazonEC2Client(credentials);
			
			// create a request for a computer
			RunInstancesRequest request = new RunInstancesRequest();
			request.setInstanceType(InstanceType.T1Micro.toString());
			request.setMinCount(1);
			request.setMaxCount(1);
			request.setImageId("ami-51792c38"); // supports java
			request.withUserData(getScript());
			
			// start instance
			manager.runInstances(request);
			
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
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
	
	public static String getScript()
	{
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash");
		lines.add("wget https://s3.amazonaws.com/akiajzfcy5fifmsaagrq/worker.jar");
		lines.add("echo Hello >> hello.txt");
		lines.add("java -jar worker.jar");
		String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
		return str;
	}
	
    static String join(Collection<String> s, String delimiter) 
    {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext())
        {
            builder.append(iter.next());
            if (!iter.hasNext())
            {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

}
