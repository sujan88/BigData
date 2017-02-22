package edu.mum.bigdata.spark.lettercount.java7.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

final public  class IsWord implements Function<String,Boolean>, Serializable{
		private static final long serialVersionUID = 1L;
	@Override
	public Boolean call( String s) throws Exception {
		char[] cArray =s.toCharArray();
		boolean valid=true;
		for(int i=0;i<cArray.length;i++)
		{
			if(!Character.isLetter(cArray[i])) valid=false;
		}
		return valid;
	}
		 
	 }