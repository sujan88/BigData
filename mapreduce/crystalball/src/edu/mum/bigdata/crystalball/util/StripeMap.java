package edu.mum.bigdata.crystalball.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class StripeMap extends MapWritable {

	


	@Override
	public int hashCode() {
		int result =31;
		result = result *  this.keySet().hashCode();
		result = result * this.values().hashCode();
		return result;
	}

	@Override
	public String toString() {
		StringBuffer sb=new  StringBuffer();
		sb.append("  { ");
		for(Writable key: this.keySet()){
			DoubleWritable count = (DoubleWritable)this.get(key);

			sb.append(key.toString());
			sb.append(" = ");
			sb.append(count.get());
			sb.append("  ");
		}
		sb.append("} ");
		return sb.toString();
	}
	
	
}
