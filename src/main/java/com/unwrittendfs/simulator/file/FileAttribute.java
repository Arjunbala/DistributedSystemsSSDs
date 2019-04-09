package com.unwrittendfs.simulator.file;

public class FileAttribute {
	
	private long mFileSize;
	private long mFileCreated;
	private long mLastModified;
	
	public FileAttribute(long createdTime) {
		mFileSize = 0;
		mFileCreated = createdTime;
		mLastModified = createdTime;
	}
	
	public long getFileSize() {
		return mFileSize;
	}
	
	public long getFileCreatedTime() {
		return mFileCreated;
	}
	
	public long getLastModifiedTime() {
		return mLastModified;
	}
	
	public boolean increaseFileSize(long timeStamp, long sizeIncrease) {
		mLastModified = timeStamp;
		mFileSize += sizeIncrease;
		return true;
	}
	
	public boolean decreaseFileSize(long timestamp, long sizeDecrease) {
		if(mFileSize < sizeDecrease) {
			return false;
		}
		mLastModified = timestamp;
		mFileSize -= sizeDecrease;
		return true;
	}
}