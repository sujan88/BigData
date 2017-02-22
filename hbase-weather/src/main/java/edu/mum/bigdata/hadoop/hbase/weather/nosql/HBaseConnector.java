package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnector {
	

	private  static Connection connection = null;
	

	public static Connection getConnection() {

		if(connection==null ||connection.isClosed())
		{
			createConnection();
		}
		return connection;
	}

	
	private static void createConnection() 
	{
			try 
			{
			connection = ConnectionFactory.createConnection( HBaseConfiguration.create() );
			}
			catch (IOException e) {
				e.printStackTrace();
			}

	}
	
	
	public  void shutDown() throws IOException
	{
		System.out.println("Connection shutdown ...");    
		connection.getAdmin().shutdown();
	}

	public static  void closeCoonection(){
		try
		{
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
