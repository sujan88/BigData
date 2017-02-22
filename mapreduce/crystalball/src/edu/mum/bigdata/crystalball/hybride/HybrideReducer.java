package edu.mum.bigdata.crystalball.hybride;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import edu.mum.bigdata.crystalball.util.Pair;
import edu.mum.bigdata.crystalball.util.StripeMap;
import edu.mum.bigdata.crystalball.util.Util;

public class HybrideReducer extends Reducer<Pair,IntWritable,Text,StripeMap>{

	int currentTerm;
	int marginal ;
	private StripeMap stripeMap;
	private Text neighbourKey;
	
	@Override
	protected void setup( Reducer<Pair, IntWritable, Text, StripeMap>.Context context)  
			throws IOException, InterruptedException 
	{
		currentTerm = 0;
		marginal = 0;
		stripeMap = new StripeMap();
	}

	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> values, Reducer<Pair, IntWritable, Text, StripeMap>.Context context)
			throws IOException, InterruptedException 
	{

		double termCount;
		if(currentTerm == 0){
			currentTerm = pair.getX().get();
		}
		else if(currentTerm != pair.getX().get() )
		{
			emitOutput(context);
			
			currentTerm = pair.getX().get();
			stripeMap = new StripeMap();
			marginal = 0;
		}
		
		for(IntWritable value : values)
		 {
			
			 neighbourKey = new Text(pair.getY().get()+"");
			 termCount = stripeMap.containsKey(neighbourKey) ? ((DoubleWritable)stripeMap.get(neighbourKey)).get() + value.get() : value.get();
			 stripeMap.put(neighbourKey, new DoubleWritable(termCount) );
			 marginal += value.get() ;
			
		 }
		
	}

	@Override
	protected void cleanup( Reducer<Pair, IntWritable, Text, StripeMap>.Context context)
			throws IOException, InterruptedException 
	{
		emitOutput(context);
	}
	
	
	private void emitOutput( Reducer<Pair, IntWritable, Text, StripeMap>.Context context) 
			throws IOException, InterruptedException
	{
		double relFreq;
		for(Writable key :  stripeMap.keySet())
		{
			neighbourKey = (Text)key;
		    relFreq = ((DoubleWritable)stripeMap.get(key)).get()/marginal;
			stripeMap.put(key, new DoubleWritable( Util.roundTwoDecimal(relFreq)) );
			
			
		}
		context.write(new Text(currentTerm+""), stripeMap);

	}
	
}