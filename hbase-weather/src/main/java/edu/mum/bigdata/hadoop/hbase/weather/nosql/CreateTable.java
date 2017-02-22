package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;


public class CreateTable {


	public static void createTable( String tableName ,List<String> columnFamilies) 
	{
		try{
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		       //column tableDescriptorfamily 
				for(String family : columnFamilies)
				{
				tableDescriptor.addFamily(new HColumnDescriptor(family));
				}
			    
				HBaseConnector.getConnection().getAdmin().createTable(tableDescriptor);
			    System.out.println(" Table created ");
		}
		catch (IOException e) {
			e.printStackTrace();
		}	    
			    
	}
}
