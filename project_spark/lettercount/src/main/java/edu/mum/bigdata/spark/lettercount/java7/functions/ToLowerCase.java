package edu.mum.bigdata.spark.lettercount.java7.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

final public  class ToLowerCase implements Function<String,String>, Serializable
{
		private static final long serialVersionUID = 1L;
	@Override
	public String call( String s) throws Exception 
	{										
		return s.toLowerCase();
	}
		 
}