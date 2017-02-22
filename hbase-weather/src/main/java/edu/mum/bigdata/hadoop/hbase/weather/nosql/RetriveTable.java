package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveTable {
	
	/**
	 *  Reading values cell
	 * @param rowId
	 * @param columnFamily
	 * @param columnName
	 */
	public static String readCell(String tableName, String rowId, String columnFamily, String columnName) throws IOException
	{
		Get get = new Get(Bytes.toBytes(rowId));
		Table table =TableExist.getTable(tableName);
	    byte []  value = table.get(get).getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
	    table.close();
		return Bytes.toString(value);
	}
	
	public static String readAll(String tableName) 
	{
	      // Getting the scan result
		  Scan scan = new Scan();
		  Table table = null;
	      ResultScanner scanner = null;
	      StringBuffer sb = new StringBuffer();
		try {
		
		if(tableName.equals(ColumnNames.CONTACTS)){
			for (String family : ColumnNames.columnfamilies_contact) {
				for (String column : ColumnNames.columns_contact) {
					scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
				}
			}
		} else if (tableName.equals(ColumnNames.WEATHER)) {
			for (String family : ColumnNames.columnfamilies_weather) {
				for (String column : ColumnNames.columns_weather) {
					scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
				}
			}
		}

			table =TableExist.getTable(tableName);
			scanner = table.getScanner(scan);

	      // Reading values from scan result
	      Result result = scanner.next();
	      while (result != null){

	    	 
	    	  if(tableName.equals(ColumnNames.CONTACTS))
	    	  {
	  			for (String family : ColumnNames.columnfamilies_contact) {
	  				for (String column : ColumnNames.columns_contact)
	  				{    
	  					 sb.append(family+" - "+column+" :     ");
	  					 sb.append(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))));
	  					 sb.append("\n");
	  				}
	  				sb.append("\n");
	  			}
	  		} else if (tableName.equals(ColumnNames.WEATHER))
	  		{
	  			sb.append("\n ROW ID  :");
	  			sb.append(Bytes.toString(result.getRow()));
	  			sb.append("\n");
	  			sb.append(ColumnNames.TEMPERATURE+"  - Celsius  ");
	  			sb.append(ColumnNames.HUMIDITY+"  - %  ");
	  			sb.append(ColumnNames.WIND_SPEED+"  - ms-1");
	  			sb.append("\n");	
	  			sb.append("\n");	
	  			for (String family : ColumnNames.columnfamilies_weather) {
	  				for (String column : ColumnNames.columns_weather) 
	  				{
	  					 sb.append(family+" - "+column+" :     ");
	  					 sb.append(Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))));
	  					 sb.append("\n");	
	  				}
	  				sb.append("\n");
	  			}
	  		}
	    	  
				result = scanner.next();

			}
		System.out.println("All row values  : \n" + sb.toString());
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally 
		{
			// closing the scanner
			scanner.close();
			try
			{
				table.close();
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

}
