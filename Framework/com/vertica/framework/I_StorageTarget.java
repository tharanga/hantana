package com.vertica.sdk;

public interface I_StorageTarget {
		public Object setupTargetWriter(String fileName, String fileExtension, String fileEncoding);
        public void closeTarget();
}