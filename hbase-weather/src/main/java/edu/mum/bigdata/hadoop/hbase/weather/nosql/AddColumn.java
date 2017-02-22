package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;

public class AddColumn {
	

	public static void addColumn( String tableName, String columnName) 
	{
		try {
		// Adding column family
		  HBaseConnector.getConnection().getAdmin().addColumn(TableName.valueOf(tableName),  new HColumnDescriptor(columnName) );
	      System.out.println("coloumn "+columnName+" is added to tabel"+tableName);
		}
		catch (IOException e) {
			e.printStackTrace();
		}	
	}
}
