package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;

public class DeleteTable {
	public static void  deleteTable( String tableName ) 
	{
		try 
		{
		TableActivation.disableTable(tableName);
		HBaseConnector.getConnection().getAdmin().deleteTable(TableName.valueOf(tableName));
	    System.out.println( "Table deleted.");
		} 
		catch (IOException e) {

			e.printStackTrace();
		}

	}
}
