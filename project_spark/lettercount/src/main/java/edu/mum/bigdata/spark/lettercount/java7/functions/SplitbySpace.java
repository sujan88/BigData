package edu.mum.bigdata.spark.lettercount.java7.functions;


import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;

final public class SplitbySpace implements FlatMapFunction<String,String>{
	private static final long serialVersionUID = 1L;


	public java.lang.Iterable<String> call(String s) throws Exception
	{
		return Arrays.asList(s.split(" ")); 
	}};
		 