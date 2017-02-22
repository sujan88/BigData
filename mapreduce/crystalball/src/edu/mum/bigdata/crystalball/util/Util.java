package edu.mum.bigdata.crystalball.util;

public class Util {
	
	public static double roundTwoDecimal(double value){
		
		return Math.round(value *10000.0)/10000.0;
	}

}
