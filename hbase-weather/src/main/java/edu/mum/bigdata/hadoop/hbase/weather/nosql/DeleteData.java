package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteData {

	/**
	 * delete cell of one row and column
	 * @param rowId
	 * @param columnFamily
	 */
	public static void deleteCell(String tableName, String rowId, String columnFamily, String columnName) 
	{
		try
		{
		  Table table =TableExist.getTable(tableName);
	      Delete delete = new Delete(Bytes.toBytes(rowId));
	      delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
	      table.delete(delete);
		  table.close();
		System.out.println("Deleted cell value from row " +rowId+" columnFamily "+columnFamily+" columnName "+columnName); 

		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
	/**
	 * delete column family of one row
	 * @param rowId
	 * @param columnFamily
	 */
	public void deleteRowFamily(String tableName, String rowId, String columnFamily) throws IOException
	{
		try
		{
		Table table = TableExist.getTable(tableName);
	    Delete delete = new Delete(Bytes.toBytes(rowId));
	    delete.addFamily(Bytes.toBytes(columnFamily));
	    table.delete(delete);
	    table.close();
		System.out.println("Data inserted."); 
	}
	catch(IOException e){
		e.printStackTrace();
	}
	}
	


}
