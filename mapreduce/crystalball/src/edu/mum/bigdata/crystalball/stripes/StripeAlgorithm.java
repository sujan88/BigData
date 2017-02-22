package edu.mum.bigdata.crystalball.stripes;

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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.mum.bigdata.crystalball.pairs.PairsAlgorithm;
import edu.mum.bigdata.crystalball.util.StripeMap;
import edu.mum.bigdata.crystalball.util.Util;



public class StripeAlgorithm extends Configured   implements Tool {
	
	

	static class StripeMapper extends Mapper<LongWritable,Text,Text,StripeMap>{

		private HashMap<String,StripeMap> parentMap;	
		private final IntWritable one = new IntWritable(1);
		private StripeMap neighbourStripe;
		private Text neighbourKey;
		private IntWritable neighbourValue;
		private String x;
		private String y;
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, StripeMap>.Context context)
				throws IOException, InterruptedException {
			parentMap = new HashMap<String,StripeMap>();
			
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, StripeMap>.Context context)
				throws IOException, InterruptedException {
		
			String[] st = value.toString().split(" ");
			
			for(int i=1 ; i< st.length; i++)  // skip the first value as its name of the customer 
			{         
				x = st[i];
				
				if(!parentMap.containsKey(x))
				{
					parentMap.put(x, new StripeMap());
				}
				
				NEIGHBOUR : 
					for(int j=i+1 ; j< st.length && i+1 <st.length ; j++){
						y = st[j];
					
					if( x.equals(y))
					{
						break NEIGHBOUR;
					}
					neighbourStripe  = parentMap.get(x);
					neighbourKey= new Text(y);
					
				    if(!neighbourStripe.containsKey(neighbourKey))
				    {
				    	neighbourStripe.put(neighbourKey, one);
					}
					else
					{ 
						neighbourValue	= (IntWritable)neighbourStripe.get( neighbourKey) ; // get the count of neighbourKey

						neighbourStripe.put(neighbourKey, new IntWritable(neighbourValue.get() + 1));
						
					}
					}
				
				if(parentMap.get(x).isEmpty()) parentMap.remove(x);
			}
		}

		
		@Override
		protected void cleanup( Mapper<LongWritable, Text, Text, StripeMap>.Context context)
				throws IOException, InterruptedException {
		for(String key : parentMap.keySet())
		{
			context.write(new Text(key), parentMap.get(key));
		}
		parentMap.clear();	
		}

	}

	

static class StripeReducer extends Reducer<Text,StripeMap,Text,StripeMap>{
	

	private Text neighbourKey;
	private IntWritable neighbourValue;
	double sumOfneighbourValue;
	private StripeMap sumOfStripe ;
	int marginal ;

	@Override
	protected void setup( Reducer<Text, StripeMap, Text, StripeMap>.Context context)
			throws IOException, InterruptedException 
	{
		sumOfStripe = new StripeMap();
	}

	@Override
	protected void reduce(Text key, Iterable<StripeMap> value, Reducer<Text, StripeMap, Text, StripeMap>.Context context)
			throws IOException, InterruptedException {
		
		sumOfStripe.clear(); 
		marginal = 0 ;
		
		for(StripeMap neighbourStripe : value){
			
			for(Writable nKey : neighbourStripe.keySet()){
				neighbourKey = (Text)nKey;
				neighbourValue =(IntWritable)neighbourStripe.get(neighbourKey);
				
				sumOfneighbourValue = 	 sumOfStripe.containsKey(neighbourKey) ? ((DoubleWritable)sumOfStripe.get(neighbourKey)).get() + neighbourValue.get():neighbourValue.get();
				
				sumOfStripe.put(neighbourKey, new DoubleWritable(sumOfneighbourValue));
				marginal += neighbourValue.get();
				
			}
			
			
		}
		
		// calculate the relative frequency of each neighbourKey
		for(Writable nKey : sumOfStripe.keySet())
		{
			neighbourKey = (Text)nKey;
			sumOfneighbourValue = ((DoubleWritable)sumOfStripe.get(neighbourKey)).get();
			
			sumOfStripe.put(neighbourKey, new DoubleWritable(Util.roundTwoDecimal(sumOfneighbourValue/marginal)) );
		}
 
		context.write(key, sumOfStripe);
	}

	}
	
static class StripePartitioner extends Partitioner<Text, StripeMap>{

	@Override
	public int getPartition(Text key, StripeMap value, int numberReducerTasks) {
		if(numberReducerTasks==0) return 0;
		else if(key.toString().charAt(0) < '5') return 0;
		else  return 1 % numberReducerTasks;
	
		
	
	}
	
}

public static void main(String[] args) throws Exception 
{
		int result = ToolRunner.run(new Configuration(),new StripeAlgorithm(), args);
		 System.exit(result);
}

@Override
public int run(String[] arg) throws Exception 
{
	Configuration conf = new Configuration();
	Job job = new Job(conf, "crystal ball Stripe algorithm");

	job.setJarByClass(StripeAlgorithm.class);

	FileInputFormat.addInputPath(job, new Path(arg[0]));
	Path outputPath = new Path(arg[1]);
	FileOutputFormat.setOutputPath(job, outputPath);

	job.setMapperClass(StripeMapper.class);
	job.setReducerClass(StripeReducer.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(StripeMap.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(StripeMap.class);
	job.setPartitionerClass(StripePartitioner.class);
	job.setNumReduceTasks(2);

	return job.waitForCompletion(true) ? 0 : 1;
}

}
