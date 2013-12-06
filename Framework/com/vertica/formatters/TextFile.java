package com.vertica.formatters;

import java.io.OutputStream;
import java.lang.reflect.Array;

public class TextFile implements IStorageFormatter{
	String fileName = "toFileOutput";
	String fileExtension = ".txt";
	String fileEncoding="utf-8";
	
	OutputStream storageTarget = null;
	
	byte[] outputBuffer;
	int maxBufferSize, currentBufferSize;
	
	public void setupFormatWriter(IStorageTarget target){
		
		setupBuffer();
		
		try{
			//Pass the type of file to be created to storage target and store the connection it creates
			storageTarget = (OutputStream) target.setupTargetWriter(fileName, fileExtension, fileEncoding);
		}catch(Exception e){}

	}
	
	public void closeFormatter(){
		try{
			//Write what's left in buffer
			writeBufferContents();
			//Close the storage target
			storageTarget.close();
		}catch(Exception e){}
	}
	
    public void setupBuffer(){
   	 	maxBufferSize = 6;
        currentBufferSize = 0;
        outputBuffer = new byte[maxBufferSize];
   }
   
   public void resetBuffer(){
	   	currentBufferSize = 0;
	   	outputBuffer = new byte[maxBufferSize];
   }
   
   public void writeToTarget(byte[] input){
	   	int inputIndex = 0;
	   	
	   	for(int leftToWrite = Array.getLength(input); leftToWrite > 0;){
	   		//Continues to loop while there are still bytes to write(in case input fills buffer several times)
	       	if(leftToWrite <= maxBufferSize - currentBufferSize){
	       		// Input will fit in buffer so copy it over
	       		for(int j = 0; j < leftToWrite; j++){
	       			outputBuffer[currentBufferSize] = input[inputIndex];
	       			inputIndex++;
	       			currentBufferSize++;
	       		}
	       		if(maxBufferSize == currentBufferSize){
	       			// Check to make sure buffer didn't get full on the last byte
	       			writeBufferContents();
	       			resetBuffer();
	       		}
	       		leftToWrite = 0; //All bytes were written so set i to zero
	       	}else{ // Input won't fit in buffer so only write part of input
	       		
	       		//Adjust how much will be left to write after this part is copied
	       		leftToWrite = leftToWrite - (maxBufferSize - currentBufferSize);
	       		//Input won't fit into buffer so only copy part of it
	       		int runTo = maxBufferSize - currentBufferSize;
	       		for(int j = 0; j < runTo; j++){
	       			outputBuffer[currentBufferSize] = input[inputIndex];
	       			inputIndex++;
	       			currentBufferSize++;
	       		}
	       		
	       		writeBufferContents();
	       		resetBuffer();
	       	}
	   	}
   }
   
   public void writeBufferContents(){
	   try{
			//Write the byte array received from the data formatter to the storage target
			storageTarget.write(outputBuffer);
		}catch(Exception e){}
   }
}