package edu.mum.bigdata.spark.lettercount.java7.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

final public class CountSumOfTerms implements PairFunction<String,String,Integer>, Serializable{

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Integer> call( String word) throws Exception {
		return new Tuple2<String, Integer>(word, 1); 
	}
	
}