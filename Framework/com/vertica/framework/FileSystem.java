package com.vertica.sdk;


import java.io.FileOutputStream;


public class FileSystem implements I_StorageTarget{
        FileOutputStream fileWriter;
        String targetAddress = "/home/dbadmin/DynamicClass/";
        
        
        public Object setupTargetWriter(String fileName, String fileExtension, String fileEncoding){
            try{
            	//Create the FileOutputStream that can write to the file system.
            	fileWriter = new FileOutputStream(targetAddress + fileName + "." + fileExtension);
    			Object o = (Object) fileWriter;
    			return o;
    		}catch(Exception e){}
    		return null;
        }	
        
        public void closeTarget(){
                
                try{
                	//Close the FileOutputStream
                	fileWriter.close();
                } catch (Exception e)
                {
                        e.printStackTrace();
                }
        }
}