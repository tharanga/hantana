package com.vertica.framework;

import java.util.ArrayList;

public interface DataFormatter {
	public String transformColumnTypes(ArrayList<String> str);
	public String transformRecord(ArrayList<String> record);
}