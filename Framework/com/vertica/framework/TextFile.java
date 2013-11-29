package com.vertica.sdk;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;

public class TextFile implements I_StorageFormatter{
	String fileName = "toFileOutput";
	String fileExtension = ".txt";
	String fileEncoding="utf-8";
	
	OutputStream storageTarget = null;
	
	public void setupFormatWriter(I_StorageTarget target){
		
		try{
			//Pass the type of file to be created to storage target and store the connection it creates
			storageTarget = (OutputStream) target.setupTargetWriter(fileName, fileExtension, fileEncoding);
		}catch(Exception e){}

	}
	
	public void writeToTarget(byte[] byteArray){
		try{
			//Write the byte array received from the data formatter to the storage target
			storageTarget.write(byteArray);
		}catch(Exception e){}
    }
	
	public void closeFormatter(){
		try{
			//Close the storage target
			storageTarget.close();
		}catch(Exception e){}
	}
}