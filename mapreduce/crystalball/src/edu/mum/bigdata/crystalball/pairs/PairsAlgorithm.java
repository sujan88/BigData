package edu.mum.bigdata.crystalball.pairs;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.mum.bigdata.crystalball.hybride.HybrideAlgorithm;
import edu.mum.bigdata.crystalball.util.Pair;
import edu.mum.bigdata.crystalball.util.Util;


public class PairsAlgorithm extends Configured   implements Tool {
	
	public static final IntWritable MINUS = new IntWritable(-1); 

static class PairMapper extends Mapper<LongWritable,Text,Pair,IntWritable>{
	  private HashMap<Pair,IntWritable>  pairMap ;
	  private Pair pair;	
	  
		@Override
		protected void setup( Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
			throws IOException, InterruptedException 
		{
			pairMap = new HashMap<Pair, IntWritable>();
	    }


		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] st = value.toString().split(" ");
			
			
			int x;
			int y;
			for(int i=1 ; i< st.length; i++){
				x = Integer.parseInt(st[i]);
				
				NEIGHBOUR : 
					for(int j=i+1 ; j< st.length && i+1 <st.length ; j++){
					y = Integer.parseInt(st[j]);
					
					if( x == y ){
						break NEIGHBOUR;
					}
				    pair = new Pair(x,y);
					
					pairMap.put(pair, new IntWritable(pairMap.containsKey(pair)?  pairMap.get(pair).get()+1 : 1));
					
					pair = new Pair(new IntWritable(x), MINUS);
					pairMap.put(pair, new IntWritable(pairMap.containsKey(pair)?  pairMap.get(pair).get()+1 : 1));

					}
			}
		}


		@Override
		protected void cleanup( Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for(Pair pair : pairMap.keySet())
			{
				context.write(pair, pairMap.get(pair));
			}
		}

}


static class PairReducer extends Reducer<Pair,IntWritable,Pair,DoubleWritable>{

	int marginal;
 // pair key is margined by symbol -1. 
	private DoubleWritable result = new DoubleWritable();
	
	@Override
	protected void setup( Reducer<Pair, IntWritable, Pair, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		marginal = 0;
	}

	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> counts, Reducer<Pair, IntWritable, Pair, DoubleWritable>.Context outPut)
			throws IOException, InterruptedException {

		
		if(MINUS.equals(pair.getY())){
			marginal = 0;
			for(IntWritable count : counts)
			{
				marginal += count.get();
			}
		}
		else
		{
			double sum = 0;
			for(IntWritable count : counts)
			{
				sum += count.get() ;
			}

			result.set(Util.roundTwoDecimal(sum/marginal));
			outPut.write(pair, result);
		}

	}

}


static class PairPartitioner extends Partitioner<Pair, IntWritable>{

	@Override
	public int getPartition(Pair key, IntWritable value, int numberReducerTasks) {
		
		if(numberReducerTasks == 0) return 0;
		else if(String.valueOf(key.getX().get()).charAt(0) < '4') return 0;
		else if(String.valueOf(key.getX().get()).charAt(0) < '7') return 1 % numberReducerTasks;
		else  return 2  % numberReducerTasks;
		
	
	}
	
}

public static void main(String[] args) throws Exception 
{
	 int result = ToolRunner.run(new Configuration(),new PairsAlgorithm(), args);
	 System.exit(result);
}

@Override
public int run(String[] arg) throws Exception {
	
	Configuration conf = new Configuration();
	Job job = new Job(conf, "crystal ball pair algorithm");

	job.setJarByClass(PairsAlgorithm.class);

	FileInputFormat.addInputPath(job, new Path( arg[0]));
	Path outputPath = new Path(arg[1]);
	FileOutputFormat.setOutputPath(job, outputPath);

	job.setMapperClass(PairMapper.class);
	job.setReducerClass(PairReducer.class);

	job.setMapOutputKeyClass(Pair.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(Pair.class);
	job.setOutputValueClass(DoubleWritable.class);

	job.setPartitionerClass(PairPartitioner.class);
	job.setNumReduceTasks(3);

 return job.waitForCompletion(true) ? 0 : 1;
}	

}
