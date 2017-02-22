package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;

public class DeleteColumn {

public static void deleteolumn( String tableName, String columnName) {
		
	try
	{
		// Adding column family
		  HBaseConnector.getConnection().getAdmin().deleteColumn(TableName.valueOf(tableName), columnName.getBytes());
	      System.out.println("coloumn "+columnName+" is DELETED from  tabel"+tableName);
	} 
	catch (IOException e)
	{
		e.printStackTrace();
	}
	}
	
}
