package com.vertica.framework;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.vertica.sdk.ColumnTypes;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.ParamReader;
import com.vertica.sdk.PartitionReader;
import com.vertica.sdk.PartitionWriter;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.TransformFunction;
import com.vertica.sdk.TransformFunctionFactory;
import com.vertica.sdk.UdfException;
import com.vertica.sdk.VerticaType;

// Break a single string input into individual words (substrings delimited by
// one or more spaces).
public class FrameworkFactory extends TransformFunctionFactory
{
        String newLine = System.getProperty("line.separator"); // Get new line character for any system that may be used
        
        String userInputDataFormatter = "";
        String userInputDestinationFormatter = "";
        
        DataFormatter dataFormatter;
        DestinationFormatter destinationFormatter;
        
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
                parameterTypes.addVarchar(100, "DestinationFormatter");
        }
        
        public class Framework extends TransformFunction
        {
                @Override
                public void setup(ServerInterface srvInterface, SizedColumnTypes argTypes){
                	srvInterface.log("IN SETUP");
                        Class<?> tempDataFormatter = null;
                        Class<?> tempDestinationFormatter = null;
                        
                        int columnCount = argTypes.getColumnCount();
                        ArrayList<String> columnTypes = new ArrayList<String>();
                        String tempStr="";
                        ParamReader paramReader = srvInterface.getParamReader();
                        
                        
                        
                        //Get parameters from select statement
                        try{
                        	userInputDataFormatter = paramReader.getString("DataFormatter").str();
                        	userInputDestinationFormatter = paramReader.getString("DestinationFormatter").str();
                        
                            tempDataFormatter = Class.forName("com.vertica.sdk." + userInputDataFormatter);
                            tempDestinationFormatter = Class.forName("com.vertica.sdk." + userInputDestinationFormatter);                                                
	                        
	                        //Setup formatters
	                        dataFormatter = (DataFormatter) tempDataFormatter.newInstance();
	                        destinationFormatter = (DestinationFormatter) tempDestinationFormatter.newInstance();
	                        
	                        //Setup any connections needed for destination formatter
	                        destinationFormatter.setupFormatter();
	                        
	                        // Create the first output row to file with column names
	                        for(int i = 0; i < columnCount-1; i++){
	                                columnTypes.add(checkType(argTypes.getColumnType(i)));
	                        }
	                        
	                        // Add last column name to list
	                        columnTypes.add(checkType(argTypes.getColumnType(columnCount-1)));
	                        
	                        //Send to DataFormatter
	                        tempStr = dataFormatter.transformColumnTypes(columnTypes);
	                        
	                        //Send to DestinationFormatter
	                        destinationFormatter.toDestination(tempStr);
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
                        destinationFormatter.closeFormatter();
                }
                
                @Override
                public void processPartition(ServerInterface srvInterface, PartitionReader inputReader,
                PartitionWriter outputWriter) throws UdfException, DestroyInvocation
                {
                        int columnCount = inputReader.typeMetaData.getColumnCount();
                        ArrayList<String> record;
                        String tempStr = "";
                        
                        // Loop over all rows passed in in this partition.
                        do {
                                record = new ArrayList<String>();
                                // Write all columns of a row to file, skipping last one so a , is not added to end
                                for(int i = 0; i < columnCount-1; i++){
                                        //record.add(writeString(inputReader, i));
                                	ByteBuffer bb = inputReader.getColRef(i);
                                }
                                //Write last column
                                //record.add(writeString(inputReader, columnCount-1));
                                
                                //Send to DataFormatter
                                tempStr = dataFormatter.transformRecord(record);
                                
                                //Send to DestinationFormatter
                                destinationFormatter.toDestination(tempStr);

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
        
        public String checkType(VerticaType column){
                if(column.isBinary())
                        return "BINARY";
                if(column.isBool())
                        return "BOOLEAN";
                if(column.isChar())
                        return "CHAR";
                if(column.isDate())
                        return "DATE";
                if(column.isFloat())
                        return "FLOAT";
                if(column.isInt())
                        return "INTEGER";
                if(column.isTimestamp())
                        return "TIMESTAMP";
                if(column.isVarbinary())
                        return "VARBINARY";
                if(column.isVarchar())
                        return "VARCHAR";
                return "UNKNOWNTYPE";
        }
        
        public String writeString(PartitionReader data, int column){
                VTypes columnType = VTypes.valueOf(checkType(data.getTypeMetaData().getColumnType(column)));
                
                /*try{
                        switch(columnType){
                        case BINARY:
                                //break;
                        case CHAR:
                        case VARBINARY:
                        case VARCHAR:
                                //getVstring corresponds to the BINARY, CHAR, VARBINARY, VARCHAR types
                                return data.getVString(column).str();
                        case BOOLEAN:
                                if(data.getBoolean(column)){
                                        return "TRUE";
                                }else{
                                        return "FALSE";
                                }
                        case DATE:
                                //Needs test to see if toString returns correct format
                                return data.getDate(column).toString();
                        case FLOAT:
                                //VerticaType only has .isFloat but .get doesn't have float option
                                //Check to see if properly converts data to string
                                return String.valueOf(data.getDouble(column));
                        case INTEGER:
                                return String.valueOf(data.getLong(column));
                        case TIMESTAMP:
                                return data.getTimestamp(column).toString();
                        case UNKNOWNTYPE:
                                return "UNKNOWNTYPE";
                        default:
                                return "NOCASEFORTYPE";
                        }
                }catch (Exception e){
                        
                }
                return "ShouldNotMakeItHere";*/
        }
        
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