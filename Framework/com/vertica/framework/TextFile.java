package com.vertica.sdk;

import java.io.FileOutputStream;
import java.io.Writer;
import java.lang.reflect.Array;

public class TextFile implements I_StorageFormatter{
	String fileName = "toFileOutput";
	String fileExtension = ".txt";
	String fileEncoding="utf-8";
	
	FileOutputStream storageTarget = null;
	
	public void setupFormatWriter(I_StorageTarget target){
		
		try{
			storageTarget = (FileOutputStream) target.setupTargetWriter(fileName, fileExtension, fileEncoding);
		}catch(Exception e){}

	}
	
	public void writeToTarget(byte[] byteArray){
    	//Convert the byte array to text so it is readable in text file then write to file
		String byteString = new String(byteArray);
		try{
			//storageTarget.write(byteString.toCharArray());
			for(int i = 0; i < Array.getLength(byteArray); i++){
        		storageTarget.write(byteArray[i]);
        	}
		}catch(Exception e){}
    }
}