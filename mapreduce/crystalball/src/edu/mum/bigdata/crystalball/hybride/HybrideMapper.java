package edu.mum.bigdata.crystalball.hybride;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.mum.bigdata.crystalball.util.Pair;

public class HybrideMapper extends Mapper<LongWritable,Text,Pair,IntWritable>{

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
