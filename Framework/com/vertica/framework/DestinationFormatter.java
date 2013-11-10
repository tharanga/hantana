package com.vertica.framework

public interface DestinationFormatter {
	public Boolean setupFormatter();
	public Boolean toDestination(String str);
	public Boolean closeFormatter();
}
