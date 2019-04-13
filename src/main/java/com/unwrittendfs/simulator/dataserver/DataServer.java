package com.unwrittendfs.simulator.dataserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataServer {
	
	public enum PageStatus {
		VALID,
		INVALID,
		FREE
	};
	
	// SSD Data Structures
	private Map<Integer, List<Integer>> mChunkToPageMapping;
	private Map<Integer, PageStatus> mFreePageList;
	
	// Statistics Structures
	private Map<Integer, Integer> mEraseMap;
	private Map<Integer, Integer> mReadMap;
	private Map<Integer, Integer> mWriteMap;
	
	// Configuration Structures
	private DataserverConfiguration mConfig;
	
	public DataServer(DataserverConfiguration config) {
		mConfig = config;
		mChunkToPageMapping = new HashMap<Integer, List<Integer>>();
		mFreePageList = new HashMap<Integer, PageStatus>();
		for(int i=0;i<config.getTotalNumPages();i++) {
			mFreePageList.put(i, PageStatus.FREE);
		}
		mEraseMap = new HashMap<Integer, Integer>();
		for(int i=0;i<config.getTotalNumPages();i++) {
			mEraseMap.put(i, 0);
		}
		mReadMap = new HashMap<Integer, Integer>();
		for(int i=0;i<config.getTotalNumPages();i++) {
			mReadMap.put(i, 0);
		}
		mWriteMap = new HashMap<Integer, Integer>();
		for(int i=0;i<config.getTotalNumPages();i++) {
			mWriteMap.put(i, 0);
		}
	}
	
	public long read(int chunk_id) {
		// TODO: Implement
		return 0;
	}
	
	public long write(int chunk_id) {
		// TODO: Implement
		return 0;
	}
	
	public boolean deleteChunks(List<Integer> chunk_ids) {
		// TODO: Implement
		return true;
	}
}