package com.vertica.sdk;


import java.io.FileOutputStream;


public class FileSystem implements I_StorageTarget{
        FileOutputStream fileWriter;
        String targetAddress = "/home/dbadmin/DynamicClass/";
        
        
        public Object setupTargetWriter(String fileName, String fileExtension, String fileEncoding){
        	
           /* try{
            	writer = (Writer) formatter;
            	//writer = new BufferedWriter(new OutputStreamWriter((FileOutputStream)formatter, encoder));
            } catch (IOException e){
                    e.printStackTrace();
            }*/
            try{
            	fileWriter = new FileOutputStream(targetAddress + fileName + "." + fileExtension);
    			Object o = (Object) fileWriter;
    			return o;
    		}catch(Exception e){}
    		return null;
        }	
        
        public void closeTarget(){
                
                try{
                	fileWriter.close();
                } catch (Exception e)
                {
                        e.printStackTrace();
                }
        }
}