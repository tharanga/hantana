package com.vertica.sdk;


public interface DataFormatter {
    public byte[] formatAfterColumn(byte[] byteArray);
    public byte[] formatAfterRecord(byte[] byteArray);
}