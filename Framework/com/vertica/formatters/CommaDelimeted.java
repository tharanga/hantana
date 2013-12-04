package com.vertica.formatters;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

public class CommaDelimeted implements IDataFormatter{
	
	IStorageFormatter storageFormatter;
	
	public void setupFormatter(IStorageFormatter storageFormatter){
		//Links the data formatter to the storage formatter so the data formatter can use it.
		this.storageFormatter = storageFormatter;
	}
        
    public void formatColumnType(byte[] byteArray){
    	String insert = ",";
    	int arraySize = Array.getLength(byteArray);
    	
    	//Create an array big enough to hold the original byte array and the separator
    	byte[] returnArray = new byte[arraySize+insert.length()];
    	
    	for(int i = 0; i < arraySize; i++){
    		//Copy each byte from original array into the new one
    		returnArray[i] = byteArray[i];
    	}
    	
    	for(int i = arraySize; i < (arraySize + insert.length()); i++){
    		//Copy the separator into the end of the array
    		returnArray[i] = (byte)insert.charAt(i - arraySize);
    	}
    	//Send byte array to Storage Formatter to be written
    	storageFormatter.writeToTarget(returnArray);
    }
    public void formatColumnTypeLast(byte[] byteArray){
    	String insert = System.getProperty("line.separator");

    	int arraySize = Array.getLength(byteArray);
    	
    	//Create an array big enough to hold the original byte array and the separator
    	byte[] returnArray = new byte[arraySize+insert.length()];
    	
    	for(int i = 0; i < arraySize; i++){
    		//Copy each byte from original array into the new one
    		returnArray[i] = byteArray[i];
    	}
    	for(int i = arraySize; i < (arraySize + insert.length()); i++){
    		//Copy the separator into the end of the array
    		returnArray[i] = (byte)insert.charAt(i - arraySize);
    	}
    	//Send byte array to Storage Formatter to be written
    	storageFormatter.writeToTarget(returnArray);
    }
    public void writeColumn(byte[][] byteArray, int columnCount){
    	//String to seperate records
    	String insert = ",";
    	//String to seperate columns
    	String insert2 = System.getProperty("line.separator");
    	
		//This for loop copies the record into a temporary byte array that is bigger so it can store separators
    	for(int i = 0; i < columnCount-1; i++){
    		//Send byte array to Storage Formatter to be written
    		storageFormatter.writeToTarget(byteArray[i]);
    		storageFormatter.writeToTarget(insert.getBytes());
    	}
    	
    	//Send byte array to Storage Formatter to be written
    	storageFormatter.writeToTarget(byteArray[columnCount-1]);
    	storageFormatter.writeToTarget(insert2.getBytes());
    }
}