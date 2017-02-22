package edu.mum.bigdata.hadoop.hbase.weather;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import edu.mum.bigdata.hadoop.hbase.weather.nosql.AddColumn;
import edu.mum.bigdata.hadoop.hbase.weather.nosql.ColumnNames;
import edu.mum.bigdata.hadoop.hbase.weather.nosql.CreateTable;
import edu.mum.bigdata.hadoop.hbase.weather.nosql.HBaseConnector;
import edu.mum.bigdata.hadoop.hbase.weather.nosql.InsertRow;
import edu.mum.bigdata.hadoop.hbase.weather.nosql.RetriveTable;
import edu.mum.bigdata.hadoop.hbase.weather.nosql.TableExist;
import edu.mum.bigdata.hadoop.hbase.weather.stubs.Weather;
import edu.mum.bigdata.hadoop.hbase.weather.stubs.WhetherResponse;


public class WeatherHbaseClient 
{
    public static void main( String[] args )
    {
    	//DeleteTable.deleteTable(ColumnNames.WEATHER);
    	String rowId=args[0];
	
		if(!TableExist.tableExist(ColumnNames.WEATHER))
		{
    	CreateTable.createTable(ColumnNames.WEATHER, ColumnNames.columnfamilies_weather);
    	AddColumn.addColumn(ColumnNames.WEATHER, ColumnNames.DESCRIPTION);
    	AddColumn.addColumn(ColumnNames.WEATHER, ColumnNames.TEMPERATURE);
    	AddColumn.addColumn(ColumnNames.WEATHER, ColumnNames.WIND_SPEED);
    	AddColumn.addColumn(ColumnNames.WEATHER, ColumnNames.HUMIDITY);
		}
//    	DeleteColumn.deleteolumn(ColumnNames.CONTACTS, ColumnNames.AGE);
//    	HBaseConnector.closeCoonection();
		 
		 
		WhetherResponse response = null;
		String description ;
    	String humidity ;
    	String windSpeed;
    	String temperature ;
    	for(String city : ColumnNames.columnfamilies_weather)
    	{
			if (ColumnNames.NEWYORK.equals(city)) {
				response = WeatherAPIClient.getWhetherbyCity(
						ColumnNames.NEWYORK, "US");
			} else if (ColumnNames.COLOMBO.equals(city)) {
				response = WeatherAPIClient.getWhetherbyCity(
						ColumnNames.COLOMBO, "LK");
			} else if (ColumnNames.LONDON.equals(city)) {
				response = WeatherAPIClient.getWhetherbyCity(
						ColumnNames.LONDON, "UK");
			}
    	 description = "No descritopn";
    	 humidity = response.getMain().getHumidity().toString();
    	 windSpeed = response.getWind().getSpeed().toString();
    	 temperature = String.valueOf(Math.abs((response.getMain().getTemp()- 273.15)*100)/100);
    	
    	for( Weather weather: response.getWeather())
		{
    		description = weather.getDescription();	
		}

    		InsertRow.insertData(ColumnNames.WEATHER, rowId, city, ColumnNames.DESCRIPTION, description); 
    		InsertRow.insertData(ColumnNames.WEATHER, rowId, city, ColumnNames.TEMPERATURE, temperature); 
    		InsertRow.insertData(ColumnNames.WEATHER, rowId, city, ColumnNames.WIND_SPEED, windSpeed); 
    		InsertRow.insertData(ColumnNames.WEATHER, rowId, city, ColumnNames.HUMIDITY, humidity); 

    	}	

    	    System.out.println("Reading all rows . . .");
    	    
    		String readValues = RetriveTable.readAll(ColumnNames.WEATHER);
    		
    		HBaseConnector.closeCoonection();

    		WeatherHbaseClient.writeToFile(readValues);
  
	}
    
    
    public static void writeToFile(String content){
    	try
    	{
    	File file = new File("output/ReadValues.txt");

		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(content);
		bw.close();

		System.out.println("Done");

	} 
    catch (IOException e) 
	{
		e.printStackTrace();
	}
}

}
