package test;

import java.util.ArrayList;
import java.util.StringTokenizer;

import models.StopWords;

public abstract class Test {

	public static void main(String[] args)
	{
		StringTokenizer itr = new StringTokenizer("couldn' t");
		System.out.println(itr.countTokens());
		while (itr.hasMoreElements()) {
			System.out.println(itr.nextElement());
			
		}
//		System.out.println(StopWords.isStopWord("/hello!"));
//		System.out.println(StopWords.isStopWord("a"));
//		System.out.println(StopWords.isStopWord("as"));
//		System.out.println(StopWords.isStopWord("faith"));
//		System.out.println(StopWords.isStopWord("which"));
		
	}

}
