package edu.mum.bigdata.crystalball.hybride;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.mum.bigdata.crystalball.util.Pair;
import edu.mum.bigdata.crystalball.util.StripeMap;

public class HybrideAlgorithm extends Configured  implements Tool {
	
	
	public static void main(String[] args) throws Exception 
	{
	 int result = ToolRunner.run(new Configuration(),new HybrideAlgorithm(), args);
	 System.exit(result);
   }

	@Override
	public int run(String[] arg) throws Exception {
		 Configuration conf = new Configuration();
			
		    Job job = new Job(conf, "crystal ball Hybride algorithm");
			
			job.setJarByClass(HybrideAlgorithm.class);

			FileInputFormat.addInputPath(job, new Path(arg[0]));
			Path outputPath = new Path( arg[1]);
			FileOutputFormat.setOutputPath(job, outputPath);
		 
			job.setMapperClass(HybrideMapper.class);
			job.setReducerClass(HybrideReducer.class);

			job.setMapOutputKeyClass(Pair.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StripeMap.class);

			job.setPartitionerClass(HybridePartitioner.class);
			job.setNumReduceTasks(3);

				return job.waitForCompletion(true) ? 0 : 1;
	}

}
