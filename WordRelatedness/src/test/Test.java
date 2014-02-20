package test;

import models.StopWords;

public abstract class Test {

	public static void main(String[] args)
	{
		System.out.println(!StopWords.isStopWord("!"));
		System.out.println(!StopWords.isStopWord("A"));
		System.out.println(!StopWords.isStopWord("faith"));
		System.out.println(!StopWords.isStopWord("which"));
	}

}
