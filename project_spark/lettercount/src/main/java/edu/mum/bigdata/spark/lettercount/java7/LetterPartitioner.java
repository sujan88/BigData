package edu.mum.bigdata.spark.lettercount.java7;

import org.apache.spark.Partitioner;

public class LetterPartitioner extends Partitioner{

	private static final long serialVersionUID = 2826008364869535176L;
  
	int numOfPartitioners;
	
	public LetterPartitioner(int numOfPartitioners) {
	this.numOfPartitioners = numOfPartitioners;
}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numOfPartitioners;
	}
	
	@Override
	public int getPartition(Object key) 
	{
		String letter = (String)key;
		if(numOfPartitioners == 0 || letter.isEmpty()) return 0;
		   int code = letter.charAt(0);
		   if(numOfPartitioners == 4)
		   {
			   if (code < 102) return 0;                             // a - e
			   else if (code < 108)   return 1 % numOfPartitioners;  // f - k
			   else if (code < 114)   return 2 % numOfPartitioners;  // l - q
			   else return 3 % numOfPartitioners;                    // r - z
		   }
		   else 
		   {
			   code = code % numOfPartitioners;
			   if (code < 0)
			   {
			    return code + this.numPartitions();
			   }
			   else
			   {
			    return code;
			   }
		  } 
	}
}