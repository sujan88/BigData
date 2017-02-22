package edu.mum.bigdata.spark.lettercount.java7.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

final public class WordCountFilter implements Function<Tuple2<String,Integer> ,Boolean> {
	
 		private static final long serialVersionUID = 1L;
 		int count_ = 0;
 		public WordCountFilter(int count_){
 			this.count_=count_;
 		}
		@Override
		public Boolean call( Tuple2<String, Integer> tuple) throws Exception {
			if( tuple._1.trim().isEmpty()) return false;
			else if ( tuple._2 < this.count_) return true;
			else return false;
		}
}
