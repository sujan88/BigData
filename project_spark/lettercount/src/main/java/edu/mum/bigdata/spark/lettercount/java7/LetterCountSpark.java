package edu.mum.bigdata.spark.lettercount.java7;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.mum.bigdata.spark.lettercount.java7.functions.CountSumOfTerms;
import edu.mum.bigdata.spark.lettercount.java7.functions.EmptyStringFilter;
import edu.mum.bigdata.spark.lettercount.java7.functions.IsWord;
import edu.mum.bigdata.spark.lettercount.java7.functions.SplitBy;
import edu.mum.bigdata.spark.lettercount.java7.functions.SplitbySpace;
import edu.mum.bigdata.spark.lettercount.java7.functions.SumOfValues;
import edu.mum.bigdata.spark.lettercount.java7.functions.ToLowerCase;
import edu.mum.bigdata.spark.lettercount.java7.functions.WordCountFilter;




public class LetterCountSpark {
	
	SparkConf conf ;
	JavaSparkContext sc;

    static Logger LOGGER = Logger.getLogger(LetterCountSpark.class);
    
	public void init(String appName, String master)
	{
	    conf = new SparkConf().setAppName(appName).setMaster(master);
		sc = new JavaSparkContext(conf);
	}


	 public void  run(String [] arg) {

	
		 String inputfileName = arg[0];
		 String outputfile = arg[1];
		 String wordcountLimit = arg[2].trim();
		 
		
		 File inputFile = new File(inputfileName);

     	 JavaPairRDD<String, Integer> wordsCountResult = sc.textFile(inputFile.getAbsolutePath())
 												    			 .flatMap(new SplitbySpace())
 												    			 .map(new ToLowerCase())
 												    			 .filter(new IsWord())
 												    			 .mapToPair(new CountSumOfTerms()).partitionBy(new LetterPartitioner(10))
 												    			 .reduceByKey(new SumOfValues()); // emit word count 
     	
     	JavaPairRDD<String, Integer>  lettersWithCountsList =	wordsCountResult
														     	.filter(new WordCountFilter(Integer.parseInt(wordcountLimit)))
														     	.flatMap(new SplitBy(""))
														     	.filter(new EmptyStringFilter())
														     	.mapToPair(new CountSumOfTerms())
														     	.partitionBy(new LetterPartitioner(4))
														     	.reduceByKey(new SumOfValues()).sortByKey();
     	
     	lettersWithCountsList.saveAsTextFile(outputfile);
  	}

     public static void main( String[] args )
     {

         LetterCountSpark a=new LetterCountSpark();
         a.init("Calculate letter count of Unique word", "local");
         a.run(args);
     }
     

	


}
