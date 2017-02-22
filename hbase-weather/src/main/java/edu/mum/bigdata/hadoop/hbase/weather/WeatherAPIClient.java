package edu.mum.bigdata.hadoop.hbase.weather;
/**
 * Created by sujan on 5/16/2016.
 */

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.mum.bigdata.hadoop.hbase.weather.city.City;
import edu.mum.bigdata.hadoop.hbase.weather.stubs.Weather;
import edu.mum.bigdata.hadoop.hbase.weather.stubs.WhetherResponse;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.List;

public class WeatherAPIClient
{

	public static String postJsonObject( URI uri )
	{
		String jsonResponse ;
		try
		{
			HttpClient httpClient = HttpClients.custom().setSSLSocketFactory( new SSLConnectionSocketFactory( SSLContexts.custom().loadTrustMaterial( null, new TrustSelfSignedStrategy() ).build() ) ).build();
			//new DefaultHttpClient();
			HttpPost postRequest = new HttpPost( uri );
			postRequest.setHeader( HTTP.CONTENT_TYPE, "application/json;charset=UTF-8" );


			HttpResponse response = httpClient.execute( postRequest );
			jsonResponse = EntityUtils.toString( response.getEntity(), "UTF-8" );

			if( !( response.getStatusLine().getStatusCode() == 201 || response.getStatusLine().getStatusCode() == 200 ) )
			{

				throw new RuntimeException( "Error in Wizz Tours service request.  \n HTTP error code : " + response.getStatusLine().getStatusCode() );
			}

			httpClient.getConnectionManager().shutdown();

		}
		catch( Exception e )
		{
			jsonResponse = e.getMessage();


		}


		return jsonResponse;
	}


	public static WhetherResponse  getWhetherbyCity(String city, String countryCode){
		URI uri = null;
		try
		{

			StringBuffer buf = new StringBuffer();
			buf.append("http://api.openweathermap.org/data/2.5/weather?q=");
			buf.append(city);
			if(countryCode!=null)
			{
				buf.append( "," );
				buf.append( countryCode );
			}
			buf.append( "," );
			buf.append("uk&appid=5480851bda385a89fc6d989a6b1f31c5");

			URL url = new URL( buf.toString() );

			uri = url.toURI();

		}

		catch( Exception e )
		{
			System.out.println( e.getMessage() );

		}

		String  jsonRes =   postJsonObject(  uri);




			System.out.println(jsonRes);


		WhetherResponse response=null;
		try
		{
			response = getObjectFromJson( jsonRes, WhetherResponse.class );
		}
		catch ( IOException e )
		{
			e.printStackTrace();
		}
		

		return response;
	}



	static String readFile( String fileName ) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		try {

			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}

		} catch (IOException e){

		}finally {
			br.close();
		}
		return sb.toString();
	}

	public static <T> T getObjectFromJson( String str, Class<T> cls ) throws IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue( str, cls );
	}


	public static List<City> getCitise(String jsonString)throws IOException{
		ObjectMapper mapper = new ObjectMapper();
		List<City> cities = mapper.readValue( jsonString, mapper.getTypeFactory().constructCollectionType( List.class, City.class));
		return cities;
}
	public  static void main(String arg[]) throws IOException
	{

		WhetherResponse res = getWhetherbyCity("Colombo","LK");

		//List<City> cities = getCitise( readFile( "file/cities.txt" ) );
		System.out.println( res.getMain());
	}
}
