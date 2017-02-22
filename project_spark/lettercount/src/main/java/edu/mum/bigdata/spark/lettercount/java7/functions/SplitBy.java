package edu.mum.bigdata.spark.lettercount.java7.functions;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

final public class SplitBy implements FlatMapFunction<Tuple2<String,Integer>,String>, Serializable{
	private static final long serialVersionUID = 1L;
	String splitter_="";
	
	public SplitBy(String splitter_) {
		this.splitter_ = splitter_;
	}

	public java.lang.Iterable<String> call(Tuple2<String,Integer> t) throws Exception
	{
		return Arrays.asList(t._1.split(this.splitter_)); 
	}
	}
		 