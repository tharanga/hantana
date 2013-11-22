package com.vertica.sdk;

import java.io.FileOutputStream;

public class TextFile implements StorageFormatter{
	String formatEncoding="utf-8";
	
	public Object setupFormatWriter(){
		
		try{
			Object o = (Object) new FileOutputStream("/home/dbadmin/DynamicClass/toFileOutput.txt");
			return o;
		}catch(Exception e){}
		return null;
	}
	
	public void toWriter(DestinationFormatter destFormatter, byte[] byteArray){
    	//Use this to convert
    	destFormatter.toDestination(byteArray);
    }
}