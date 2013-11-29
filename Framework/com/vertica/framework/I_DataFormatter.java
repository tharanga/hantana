package com.vertica.sdk;

import java.nio.ByteBuffer;

public interface I_DataFormatter {
	public void setupFormatter(I_StorageFormatter storageFormatter);
    public void formatColumnType(byte[] byteArray);
    public void formatColumnTypeLast(byte[] byteArray);
    public void writeColumn(byte[][] byteArray, int columnCount);
}