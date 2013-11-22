package com.vertica.sdk;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Array;


public class ToFile implements DestinationFormatter{
        Writer writer = null;
        
        public void setupFormatter(Object formatter, String encoder){
                try{
                        writer = new BufferedWriter(new OutputStreamWriter((FileOutputStream)formatter, encoder));
                } catch (IOException e){
                        e.printStackTrace();
                }
        }
        
        public Boolean toDestination(byte[] byteArray){
                try{
                	for(int i = 0; i < Array.getLength(byteArray); i++){
                		writer.write(byteArray[i]);
                	}
                        
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