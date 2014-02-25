package test;

import java.util.ArrayList;

import models.StopWords;

public abstract class Test {

	public static void main(String[] args)
	{
		System.out.println(StopWords.isStopWord("/hello!"));
		System.out.println(StopWords.isStopWord("a"));
		System.out.println(StopWords.isStopWord("as"));
		System.out.println(StopWords.isStopWord("faith"));
		System.out.println(StopWords.isStopWord("which"));
		
	}

}
