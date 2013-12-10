package manager;

import java.util.LinkedList;

public class Task
{
	// information from local application
	String localApplicationId;
	int n;
	String outputFileKey;
	String imageUrlKey;
	
	// information gathered from manager
	int numberOfImages;
	int numberOfImagesStamped;
	LinkedList<String> oldUrls;
	LinkedList<String> newUrls;
	
	public Task(String localApplicationId, int n, String outputFileKey, String imageUrlKey)
	{
		this.localApplicationId = localApplicationId;
		this.n = n;
		this.outputFileKey = outputFileKey;
		this.imageUrlKey = imageUrlKey;
	}	
	

}
