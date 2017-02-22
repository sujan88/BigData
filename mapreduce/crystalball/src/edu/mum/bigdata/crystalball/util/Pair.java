package edu.mum.bigdata.crystalball.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;



public class Pair implements WritableComparable<Pair> {

	private IntWritable x;
	private IntWritable y;
	
	@Override
	public String toString() {
		return "Pair [x=" + x.get()+ ", y=" + y.get() + "]";
	}


	
	public Pair() {
	  x = new IntWritable(0);
	  y = new IntWritable(0);
	}
	public Pair(IntWritable x, IntWritable y) {
		this.x = x;
		this.y = y;
	}

	public Pair(int x, int y) {
		this.x= new IntWritable(x);
		this.y= new IntWritable(y);
	}
	
	public void setX(IntWritable x) {
		this.x = x;
	}

	public IntWritable getX() {
		return x;
	}

	public IntWritable getY() {
		return y;
	}



	public void setY(IntWritable y) {
		this.y = y;
	}

	
	
	@Override
	public int hashCode() {
		int result = 31;
		result = result * 17+ this.x.hashCode();
		result = result * 17 + this.y.hashCode();
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj==null || !(obj instanceof Pair)) return false;
		
	  Pair pair =(Pair)obj;
	  if(this.x.equals(pair.x) && this.y.equals(pair.y))
	  {
		  return true;
	  }
		return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x.readFields(in);
		y.readFields(in);
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		x.write(out);
		y.write(out);
	}

	@Override
	public int compareTo(Pair p) {
		int cmp = this.x.compareTo(p.getX());
		if(cmp!=0)
			return cmp;
		return this.y.compareTo(p.getY());
		
	}

}
