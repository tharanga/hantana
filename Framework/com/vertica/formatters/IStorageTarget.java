package com.vertica.formatters;

public interface IStorageTarget {
		public Object setupTargetWriter(String fileName, String fileExtension, String fileEncoding);
        public void closeTarget();
}