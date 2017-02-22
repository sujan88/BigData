package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

public class TableExist {


	public  static boolean tableExist( String tableName ) 
	{
		boolean exist = false;
		try
		{
		exist = HBaseConnector.getConnection().getAdmin().tableExists(TableName.valueOf(tableName));
	    System.out.println( exist);
		}
	    catch (IOException e) 
		{
			e.printStackTrace();
		}
	    return exist;
	}
	
	public static Table getTable( String tableName ) throws IOException
	{
		Table table = null;
		try
		{
		table= HBaseConnector.getConnection().getTable(TableName.valueOf(tableName));
		} 
		catch (IOException e) 
			{
				e.printStackTrace();
			}
		return table;
	}
	
		
	
}
