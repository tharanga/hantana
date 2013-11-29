package com.vertica.framework;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
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
                        
                        try{
                        	//Get parameters from select statement
                        	userInputDataFormatter = paramReader.getString("DataFormatter").str();
                        	userInputStorageFormatter = paramReader.getString("StorageFormatter").str();
                        	userInputDestinationFormatter = paramReader.getString("DestinationFormatter").str();
                        
                        	//Find classes for the parameters given
                            tempDataFormatter = Class.forName("com.vertica.sdk." + userInputDataFormatter);
                            tempStorageFormatter = Class.forName("com.vertica.sdk." + userInputStorageFormatter);
                            tempDestinationFormatter = Class.forName("com.vertica.sdk." + userInputDestinationFormatter);                                                
	                        
	                        //Setup formatters
	                        dataFormatter = (I_DataFormatter) tempDataFormatter.newInstance();
	                        storageFormatter = (I_StorageFormatter) tempStorageFormatter.newInstance();
	                        storageTarget = (I_StorageTarget) tempDestinationFormatter.newInstance();
	                        
	                        //Link the data formatter to storage formatter so it can use storage formatter.
	                        dataFormatter.setupFormatter(storageFormatter);
	                        //Link the storage formatter to the storage target so it can use the storage target.
	                        storageFormatter.setupFormatWriter(storageTarget);
	                        
	                        // Send as the first row to storage the column types that are being stored
	                        for(int i = 0; i < columnCount-1; i++){
	                        	//Get column type and send it to the data formatter to have it stored in storage target
	                        	dataFormatter.formatColumnType(checkType(argTypes.getColumnType(i)));
	                        }
	                        
	                        // Add last column name to list
	                        //Get column type and send it to the data formatter to have it stored in storage target
                        	dataFormatter.formatColumnTypeLast(checkType(argTypes.getColumnType(columnCount-1)));

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
                        ByteBuffer[] bbArray = new ByteBuffer[columnCount];
                        byte[][] byteArray = new byte[columnCount][];
                        int[] recordSize = new int[columnCount];
                        
                        //This iterates for each column, storing its ByteBuffer, record size and creating array to
                        //temporarily hold each record in the column
                        for(int i = 0; i < columnCount; i++){
                        	//Get the ByteBuffers for each column and store them in an array
                        	bbArray[i] = inputReader.getColRef(i);
                        	//Get the number of bytes each record would contain in the columns so they can be separated
                        	recordSize[i] = inputReader.typeMetaData.getColumnType(i).getMaxSize();
                        	//Create the byte array that will hold each record
                        	byteArray[i] = new byte[recordSize[i]];
                        }
                        
                        //Check to see if data still in ByteBuffer(Meaning another record still in buffer to write)
                        //Use index 0 as reference since there should be at least 1 column and when it is empty
                        //all other ByteBuffers will be empty
                        while(bbArray[0].remaining() > 0){
                        	for(int i = 0; i < columnCount; i++){
                        		//Transfer bytes in ByteBuffer bbArray to byteArray
                        		//This is retrieving the next record in the column for all columns by way of the loop
                        		//over the array of ByteBuffers in bbArray
                        		bbArray[i].get(byteArray[i],0,recordSize[i]);
                        	}
                        	
                        	//Send byteArray which contains an array of byte arrays which each contain a column
                        	//for one particular record to be written
                        	dataFormatter.writeColumn(byteArray, columnCount);
                        }
                        
                        //Below commented out code was just a test reading the output file
                        /*String bin="notinitialized";
                        String strArray[] = new String[5];
                        
                        try{
                                 FileReader checkOutput = 
                                          new FileReader("/home/dbadmin/DynamicClass/toFileOutput1.txt"); 
                                 
                                 BufferedReader br = new BufferedReader(checkOutput);
                                 
                                 bin = br.readLine();
                                 bin = br.readLine();
                                 
                        }catch(Exception e){
                                
                        }
                        
                        outputWriter.setString(0, bin);
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
}