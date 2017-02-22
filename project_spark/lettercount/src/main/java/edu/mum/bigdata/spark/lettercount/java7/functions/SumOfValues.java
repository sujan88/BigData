package edu.mum.bigdata.spark.lettercount.java7.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

final public class SumOfValues implements Function2<Integer,Integer,Integer>, Serializable{
	private static final long serialVersionUID = 1L;
	@Override
	public Integer call( Integer v1, Integer v2) throws Exception {
		return v1+v2;
	}
	 
 };
