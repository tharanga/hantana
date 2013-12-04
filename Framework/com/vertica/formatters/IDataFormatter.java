package com.vertica.formatters;

import java.nio.ByteBuffer;

public interface IDataFormatter {
	public void setupFormatter(IStorageFormatter storageFormatter);
    public void formatColumnType(byte[] byteArray);
    public void formatColumnTypeLast(byte[] byteArray);
    public void writeColumn(byte[][] byteArray, int columnCount);
}