package com.vertica.sdk;

public interface I_DataFormatter {
    public byte[] formatAfterColumn(byte[] byteArray);
    public byte[] formatAfterRecord(byte[] byteArray);
}