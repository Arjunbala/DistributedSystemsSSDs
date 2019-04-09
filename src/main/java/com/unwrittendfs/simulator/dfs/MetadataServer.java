package com.unwrittendfs.simulator.dfs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.file.FileAttribute;

public class MetadataServer {
	
	private Map<String, Integer> mFileDescriptorMapping;
	private Map<Integer, List<Integer>> mFdToChunkMapping;
	private Map<Integer, List<DataLocation>> mChunkIdToDataServerMapping;
	private Map<Integer, FileAttribute> mFileAttributeMapping;
	private Map<Integer, Map<Integer, Long>> mClientFilePointerMapping;
	
	public MetadataServer() {
		mFileDescriptorMapping = new HashMap<String, Integer>();
		mFdToChunkMapping = new HashMap<Integer, List<Integer>>();
		mChunkIdToDataServerMapping = new HashMap<Integer, List<DataLocation>>();
		mFileAttributeMapping = new HashMap<Integer, FileAttribute>();
		mClientFilePointerMapping = new HashMap<Integer, Map<Integer, Long>>();
	}
}