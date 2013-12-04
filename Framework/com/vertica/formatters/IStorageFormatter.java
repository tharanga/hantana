package com.vertica.formatters;

import java.util.ArrayList;

public interface IStorageFormatter {
	String formatEncoding = null;
    public void setupFormatWriter(IStorageTarget target);
    public void writeToTarget(byte[] byteArray);
    public void closeFormatter();
}