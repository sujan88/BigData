package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertRow {



	public static void insertData(String tableName,String rowId, String columnFamily, String columnName, String data)
	{
		try
		{
		Put put = new Put(Bytes.toBytes(rowId));
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(data));
		Table table = TableExist.getTable(tableName);
		table.put(put);
		table.close();
		System.out.println("Data inserted."); 
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	

}
