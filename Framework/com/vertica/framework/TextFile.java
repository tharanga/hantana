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
			storageTarget = (OutputStream) target.setupTargetWriter(fileName, fileExtension, fileEncoding);
		}catch(Exception e){}

	}
	
	public void writeToTarget(byte[] byteArray){
    	//Convert the byte array to text so it is readable in text file then write to file
		String byteString = new String(byteArray);
		try{
			//storageTarget.write(byteString.toCharArray());
			storageTarget.write(byteArray);
		}catch(Exception e){}
    }
	
	public void closeFormatter(){
		try{
			storageTarget.close();
		}catch(Exception e){}
	}
}