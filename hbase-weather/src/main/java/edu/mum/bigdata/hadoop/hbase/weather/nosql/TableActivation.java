package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

public class TableActivation 
{
	
	public  static void disableAllTable( )
 {

		HTableDescriptor[] tableDescriptorList;
		try {
			Connection con = HBaseConnector.getConnection();
			tableDescriptorList = con.getAdmin().listTables();

			// Delete the table whichadmin set as disable.
			for (int i = 0; i < tableDescriptorList.length; i++) {
				HTableDescriptor td = tableDescriptorList[i];
				System.out.println(td.getNameAsString());

				if (!con.getAdmin().isTableDisabled(td.getTableName())) 
				{
					con.getAdmin().disableTable(td.getTableName());
					System.out.println("Disbaled Table : " + td.getTableName());
				} 
				else {
					System.out.println("Table : " + td.getTableName() + " is already Disbaled");
				}
			}
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}

	
	public static void disableTable( String name ) 
	{
		try 
		{
	         if(!HBaseConnector.getConnection().getAdmin().isTableDisabled(TableName.valueOf(name)))
	         {
	        	 HBaseConnector.getConnection().getAdmin().disableTable(TableName.valueOf(name));
	        	 System.out.println("Disbaled Table : " +TableName.valueOf(name));
	         }
	         else
	         {
	        	 System.out.println("Table : " +name+" is already Disbaled");
	         }
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public static void enableTable( String name ) 
	{
		try{
	         if(HBaseConnector.getConnection().getAdmin().isTableDisabled(TableName.valueOf(name)))
	         {
	        	 HBaseConnector.getConnection().getAdmin().enableTable(TableName.valueOf(name));
	        	 System.out.println("Enabled Table : " +TableName.valueOf(name));
	         }
	         else
	         {
	        	 System.out.println("Table : " +name+" is already Enabled");
	         }
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}

}
