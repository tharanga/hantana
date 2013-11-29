package com.vertica.framework;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.vertica.sdk.*;

// Break a single string input into individual words (substrings delimited by
// one or more spaces).
public class FrameworkFactory extends TransformFunctionFactory
{
        String newLine = System.getProperty("line.separator"); // Get new line character for any system that may be used
        
        String userInputDataFormatter = "";
        String userInputStorageFormatter ="";
        String userInputDestinationFormatter = "";
        
        I_DataFormatter dataFormatter;
        I_StorageFormatter storageFormatter;
        I_StorageTarget storageTarget;
        
        // Set the number and data types of the columns in the input and output rows.
        @Override
        public void getPrototype(ServerInterface srvInterface,
        ColumnTypes argTypes, ColumnTypes returnType)
        {
                // Can take any number and type of arguments
                argTypes.addAny();
                // One column in the output row: a Varchar
                returnType.addAny();
        }
        
        // Set the width of any variable-width output columns, and also name
        // the columns.
        @Override
        public void getReturnType(ServerInterface srvInterface, SizedColumnTypes
        inputTypes, SizedColumnTypes outputTypes)
        {                
                // Set the maximum width of the return column to the width
                // of the input column and name the output column "Tokens"
                outputTypes.addVarchar(5, "NoOutput");
        }
        
        //Define parameters the framework can take
        //This is where the user of the framework will input which formatters to use
        public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes)
        {
        // Two Varchar parameters named DataFormatter and DestinationFormatter
                parameterTypes.addVarchar(100, "DataFormatter");
                parameterTypes.addVarchar(100, "StorageFormatter");
                parameterTypes.addVarchar(100, "DestinationFormatter");
        }
        
        public class Framework extends TransformFunction
        {
                @Override
                public void setup(ServerInterface srvInterface, SizedColumnTypes argTypes){
                	srvInterface.log("IN SETUP");
                        Class<?> tempDataFormatter = null;
                        Class<?> tempStorageFormatter = null;
                        Class<?> tempDestinationFormatter = null;
                        
                        int columnCount = argTypes.getColumnCount();
                        ArrayList<String> columnTypes = new ArrayList<String>();
                        String tempStr="";
                        ParamReader paramReader = srvInterface.getParamReader();
                        byte[] byteArray;
                        
                        
                        
                        //Get parameters from select statement
                        try{
                        	userInputDataFormatter = paramReader.getString("DataFormatter").str();
                        	userInputStorageFormatter = paramReader.getString("StorageFormatter").str();
                        	userInputDestinationFormatter = paramReader.getString("DestinationFormatter").str();
                        
                            tempDataFormatter = Class.forName("com.vertica.sdk." + userInputDataFormatter);
                            tempStorageFormatter = Class.forName("com.vertica.sdk." + userInputStorageFormatter);
                            tempDestinationFormatter = Class.forName("com.vertica.sdk." + userInputDestinationFormatter);                                                
	                        
	                        //Setup formatters
	                        dataFormatter = (I_DataFormatter) tempDataFormatter.newInstance();
	                        storageFormatter = (I_StorageFormatter) tempStorageFormatter.newInstance();
	                        storageTarget = (I_StorageTarget) tempDestinationFormatter.newInstance();
	                        
	                        //Setup any connections needed for the storage formatter to use the storage target.
	                        storageFormatter.setupFormatWriter(storageTarget);
	                        
	                        // Send as the first row to storage the column types that are being stored
	                        for(int i = 0; i < columnCount-1; i++){
	                        	//Get column type and send it to the data formatter
	                        	byteArray = dataFormatter.formatAfterColumn(checkType(argTypes.getColumnType(i)));
	                        	//Send byte array to storage formatter to be stored
	                        	storageFormatter.writeToTarget(byteArray);
	                        }
	                        
	                        // Add last column name to list
	                        //Get column type and send it to the data formatter
                        	byteArray = dataFormatter.formatAfterRecord(checkType(argTypes.getColumnType(columnCount-1)));
                        	//Send byte array to storage formatter to be stored
                        	storageFormatter.writeToTarget(byteArray);

                        }catch (ClassNotFoundException e){
                            throw new IllegalArgumentException("Class was not found: " + e.getMessage());
	                    }catch (IllegalAccessException e){
	                        throw new IllegalArgumentException("Error: " + e.getMessage());
	                    }catch (InstantiationException e){
	                        throw new IllegalArgumentException("Error: " + e.getMessage());
	                    }catch (Exception e){
	                    	throw new IllegalArgumentException("Error: " + e.getMessage());
	                    }
                        
                        
                }
                
                public void destroy (ServerInterface srvInterface, SizedColumnTypes argTypes){ 
                        //Close the destination
                		storageFormatter.closeFormatter();
                        storageTarget.closeTarget();
                }
                
                @Override
                public void processPartition(ServerInterface srvInterface, PartitionReader inputReader,
                PartitionWriter outputWriter) throws UdfException, DestroyInvocation
                {
                        int columnCount = inputReader.typeMetaData.getColumnCount();
                        byte[] byteArray = null;
                        ByteBuffer bb;
                        
                        // Loop over all rows passed in in this partition.
                        do {
                                // Write all columns of a row to file, skipping last one so a , is not added to end
                                for(int i = 0; i < columnCount-1; i++){
                                    //Retrieve the columns byte buffer
                                	bb = inputReader.getColRef(i);
                                	byteArray = new byte[bb.remaining()];
                                	//Transfer ByteBuffer to byteArray
                                	bb.get(byteArray);
                                	// Send ByteBuffer to formatter to be formatted and stored with false meaning not end of record
                                	storageFormatter.writeToTarget(byteArray);
                                	
                                }
                                //Write last column
                                //Retrieve the columns byte buffer
                            	bb = inputReader.getColRef(columnCount-1);
                            	byteArray = new byte[bb.remaining()];
                            	//Transfer ByteBuffer to byteArray
                            	bb.get(byteArray);
                            	// Send ByteBuffer to formatter to be formatted and stored with false meaning not end of record
                            	storageFormatter.writeToTarget(byteArray);

                                // Loop until there are no more input rows in partition.
                        } while (inputReader.next());
                        
                        //Below commented out code was just a test reading the output file
                        /*String bin;
                        String strArray[] = new String[5];
                        
                        try{
                                 FileReader checkOutput = 
                                          new FileReader("/home/dbadmin/Desktop/toFileOutput.dat"); 
                                 
                                 BufferedReader br = new BufferedReader(checkOutput);
                                 
                                 bin = br.readLine();
                                 while(bin != null){
                                         if(bin.contains("BINARY")){
                                                 strArray = bin.split(" ");
                                         }
                                         bin = br.readLine();
                                 }
                                 
                        }catch(Exception e){
                                
                        }
                        
                        outputWriter.setString(0, strArray[1]);
                        outputWriter.next();*/
                }
        }
        
        @Override
        public TransformFunction createTransformFunction(ServerInterface
        srvInterface)
        { return new Framework(); }
        
        public byte[] checkType(VerticaType column){
                if(column.isBinary())
                        return "BINARY".getBytes();
                if(column.isBool())
                        return "BOOLEAN".getBytes();
                if(column.isChar())
                        return "CHAR".getBytes();
                if(column.isDate())
                        return "DATE".getBytes();
                if(column.isFloat())
                        return "FLOAT".getBytes();
                if(column.isInt())
                        return "INTEGER".getBytes();
                if(column.isTimestamp())
                        return "TIMESTAMP".getBytes();
                if(column.isVarbinary())
                        return "VARBINARY".getBytes();
                if(column.isVarchar())
                        return "VARCHAR".getBytes();
                return "UNKNOWNTYPE".getBytes();
        }
        
        /*public void sendToFormatter(ByteBuffer bb, Boolean endRecord){
        	int bufferSize = 8; //For now we'll just read 8 bytes at a time from buffer
            byte[] byteArray = new byte[bufferSize];
            
            while(bb.remaining()>=bufferSize){ //While the ByteBuffer still has enough bytes to fill byteArray
            	bb.get(byteArray, 0, bufferSize);
            	destinationFormatter.toDestination(byteArray);
            }
            //Send the rest of buffer if any is left
            if (bb.remaining() != 0){
            	byteArray = new byte[bb.remaining()];
            	bb.get(byteArray, 0, bb.remaining());
            	
            	if(endRecord){
            		//End of record so send to formatAfterRecord
            		byteArray = dataFormatter.formatAfterRecord(byteArray);
            	}else{
            		//End of column so send to formatAfterColumn
            		byteArray = dataFormatter.formatAfterColumn(byteArray);
            	}
            	
            	//Send formatted data to be stored
            	storageFormatter.toWriter(destinationFormatter, byteArray);
            }
        	/*byte[] byteArray = new byte[bb.remaining()];
        	int size = bb.remaining();
        	int countSize = 0;
        	bb.get(byteArray, 0, bb.remaining());
        	
        	for(int i = 0; i < size; i++){
        		if(byteArray[i] != 0){
        			countSize++;
        		}
        	}
        	byte[] submitArray = new byte[countSize];
        	int nextByte = 0;
        	for(int i = 0; i < size; i++){
        		if(byteArray[i] != 0){
        			submitArray[nextByte] = byteArray[i];
        			nextByte++;
        		}
        	}
        	
        	if(endRecord){
        		submitArray = dataFormatter.formatAfterColumn(submitArray);
        	}else{
        		submitArray = dataFormatter.formatAfterRecord(submitArray);
        	}
        	
        	storageFormatter.toWriter(destinationFormatter, submitArray);
        }*/
        
        public enum VTypes {
                BINARY,
                BOOLEAN,
                CHAR,
                DATE,
                FLOAT,
                INTEGER,
                TIMESTAMP,
                VARBINARY,
                VARCHAR,
                UNKNOWNTYPE
        }
}