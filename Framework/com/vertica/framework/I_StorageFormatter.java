package com.vertica.sdk;

import java.util.ArrayList;

public interface I_StorageFormatter {
	String formatEncoding = null;
    public void setupFormatWriter(I_StorageTarget target);
    public void writeToTarget(byte[] byteArray);
    public void closeFormatter();
}