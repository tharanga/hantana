package com.vertica.sdk;

import java.util.ArrayList;

public interface StorageFormatter {
	String formatEncoding = null;
    public Object setupFormatWriter();
    public void toWriter(DestinationFormatter destFormatter, byte[] byteArray);
    
}