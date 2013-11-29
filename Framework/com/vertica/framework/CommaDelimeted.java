package com.vertica.sdk;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class CommaDelimeted implements I_DataFormatter{
        
    public byte[] formatAfterColumn(byte[] byteArray){
    	String insert = ",";
    	int arraySize = Array.getLength(byteArray);
    	
    	byte[] returnArray = new byte[arraySize+insert.length()];
    	
    	for(int i = 0; i < arraySize; i++){
    		returnArray[i] = byteArray[i];
    	}
    	for(int i = arraySize; i < (arraySize + insert.length()); i++){
    		returnArray[i] = (byte)insert.charAt(i - arraySize);
    	}
    	return returnArray;
    }
    public byte[] formatAfterRecord(byte[] byteArray){
    	String insert = System.getProperty("line.separator");
    	
    	int arraySize = Array.getLength(byteArray);
    	
    	byte[] returnArray = new byte[arraySize+insert.length()];
    	
    	for(int i = 0; i < arraySize; i++){
    		returnArray[i] = byteArray[i];
    	}
    	for(int i = arraySize; i < (arraySize + insert.length()); i++){
    		returnArray[i] = (byte)insert.charAt(i - arraySize);
    	}
    	return returnArray;
    }
}