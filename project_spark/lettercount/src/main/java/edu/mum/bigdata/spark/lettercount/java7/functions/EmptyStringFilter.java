package edu.mum.bigdata.spark.lettercount.java7.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

final public class EmptyStringFilter implements Function<String ,Boolean> {
	
 		private static final long serialVersionUID = 1L;

 		public EmptyStringFilter(){

 		}
		@Override
		public Boolean call( String  s) throws Exception {
			return !s.trim().isEmpty();

		}
}
