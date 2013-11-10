package com.vertica.framework;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;


public class ToFile implements DestinationFormatter{
	Writer writer = null;
	String newLine = System.getProperty("line.separator"); // Get new line character for any system that may be used
	
	public Boolean setupFormatter(){
		try{
			writer = new BufferedWriter(new OutputStreamWriter(
		          new FileOutputStream("/home/dbadmin/DynamicClass/toFileOutput.txt"), "utf-8"));
		} catch (IOException e){
			e.printStackTrace();
		}
		
		
		return null;
	}
	
	public Boolean toDestination(String str){
		
		try{
			writer.write(str + newLine);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return true;
	}
	
	public Boolean closeFormatter(){
		
		try{
			writer.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return true;
	}
}
