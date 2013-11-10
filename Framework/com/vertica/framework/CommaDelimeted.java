package com.vertica.framework;

import java.util.ArrayList;

public class CommaDelimeted implements DataFormatter{
	
	//*****************************************************
	//Is it possible to make these methods return any anything(array, string, integer)
	//According to whatever programmer wants to return and have framework
	//Handle this return no matter what it is?
	//****************************************************
	
	public String transformColumnTypes(ArrayList<String> record){
		StringBuilder str = new StringBuilder();
		
		for(int i =0; i <record.size()-1; i++){
			str.append(record.get(i) + ",");
		}
		
		//Append last record without comma
		str.append(record.get(record.size()-1));
		
		return str.toString();
	}
	
	public String transformRecord(ArrayList<String> record){		
		StringBuilder str = new StringBuilder();
		
		for(int i =0; i <record.size()-1; i++){
			str.append(record.get(i) + ",");
		}
		
		//Append last record without comma
		str.append(record.get(record.size()-1));
		
		return str.toString();
	}
	
	public String transformRecord(){
		return "class record without parameters";
	}
	
}