package com.vertica.sdk;

public interface DestinationFormatter {
		public void setupFormatter(Object formatter, String encoder);
        public Boolean toDestination(byte[] byteArray);
        public Boolean closeFormatter();
}