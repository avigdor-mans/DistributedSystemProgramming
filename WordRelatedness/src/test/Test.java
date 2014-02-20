package test;

import java.util.ArrayList;

import models.StopWords;

public abstract class Test {

	public static void main(String[] args)
	{
//		System.out.println(!StopWords.isStopWord("!"));
//		System.out.println(!StopWords.isStopWord("A"));
//		System.out.println(!StopWords.isStopWord("faith"));
//		System.out.println(!StopWords.isStopWord("which"));
		
		ArrayList<String> arr = new ArrayList<>();
		arr.add("assaf");
		
		arr = new ArrayList<>();
		arr.add("dror");
		
		for (String str : arr)
		{
			System.out.println(str);
		}
	}

}
