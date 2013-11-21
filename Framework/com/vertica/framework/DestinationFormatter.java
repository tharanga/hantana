package com.vertica.sdk;

public interface DestinationFormatter {
        public Boolean setupFormatter();
        public Boolean toDestination(String str);
        public Boolean closeFormatter();
}