package edu.mum.bigdata.crystalball.hybride;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.mum.bigdata.crystalball.util.Pair;

public class HybridePartitioner extends Partitioner<Pair, IntWritable>{

	@Override
	public int getPartition(Pair key, IntWritable value, int numberReducerTasks) {
		
		if(numberReducerTasks==0) return 0;
		if(String.valueOf(key.getX().get()).charAt(0) < '3') return 0;
		else if(String.valueOf(key.getX().get()).charAt(0) < '7') return 1  % numberReducerTasks;
		else  return 2 % numberReducerTasks;

	
	}
	
}