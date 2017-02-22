package edu.mum.bigdata.hadoop.hbase.weather.nosql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ColumnNames {
	public static final String NAME = "name";
	public static final String EMAIL = "email";
	public static final String PHONE = "phone";
	public static final String AGE = "age";
	public static final String PERSONAL = "personal";
	public static final String OFFICE = "office";
	public static final List<String> columnfamilies_contact = Arrays.asList(PERSONAL,OFFICE);
	public static final List<String> columns_contact= Arrays.asList(NAME,EMAIL,PHONE);
	
	public static final String LONDON = "london";
	public static final String NEWYORK = "newyork";
	public static final String COLOMBO="colombo";
	
	public static final String DESCRIPTION= "description";
	public static final String TEMPERATURE = "tempereture";
	public static final String WIND_SPEED= "wind_Speed";
	public static final String HUMIDITY= "humidity";
	
	public static final List<String> columnfamilies_weather = Arrays.asList(LONDON,NEWYORK,COLOMBO);
	public static final List<String> columns_weather= Arrays.asList(DESCRIPTION,TEMPERATURE,WIND_SPEED,HUMIDITY);
	
	public static final String WEATHER= "weather";
	public static final String CONTACTS= "contacts";
}
