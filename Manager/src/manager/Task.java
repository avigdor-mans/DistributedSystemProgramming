package manager;

import java.util.LinkedList;

public class Task
{
	// information from local application
	String localApplicationId;
	public int n;
	public String imageUrlKey;
	public String outputFileKey;
	
	// information gathered from manager
	private int numberOfImages;
	private int numberOfImagesStamped;
	private LinkedList<String> oldUrls;
	private LinkedList<String> newUrls;
	
	public Task(String localApplicationId, int n, String imageUrlKey, String outputFileKey)
	{
		this.localApplicationId = localApplicationId;
		this.n = n;
		this.imageUrlKey = imageUrlKey;
		this.outputFileKey = outputFileKey;
		
		this.numberOfImages = 0;
		this.numberOfImagesStamped = 0;
		this.oldUrls = new LinkedList<String>();
		this.newUrls = new LinkedList<String>();
	}
	
	public boolean isReady()
	{
		return numberOfImagesStamped == numberOfImages;
	}

	public int getNumberOfImages() 
	{
		return numberOfImages;
	}

	public void setNumberOfImages(int numberOfImages) 
	{
		this.numberOfImages = numberOfImages;
	}

	public int getNumberOfImagesStamped()
	{
		return numberOfImagesStamped;
	}

	public void incNumberOfImagesStamped() 
	{
		this.numberOfImagesStamped++ ;
	}

	public LinkedList<String> getOldUrls() 
	{
		return oldUrls;
	}

	public void setOldUrls(LinkedList<String> oldUrls)
	{
		this.oldUrls = oldUrls;
	}

	public LinkedList<String> getNewUrls()
	{
		return newUrls;
	}

	public void setNewUrls(LinkedList<String> newUrls)
	{
		this.newUrls = newUrls;
	}	

}
