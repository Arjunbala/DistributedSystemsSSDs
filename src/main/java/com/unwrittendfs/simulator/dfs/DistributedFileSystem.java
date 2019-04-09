package com.unwrittendfs.simulator.dfs;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.file.FileAttribute;
import com.unwrittendfs.simulator.utils.ConfigUtils;

public class DistributedFileSystem {
	
	private static DistributedFileSystem sInstance = null;
	private MetadataServer mMetadataServer;
	private ClusterConfiguration mClusterConfiguration;
	private Map<Integer, DataServer> mDataServerMap;
	
	private DistributedFileSystem(ClusterConfiguration config) {
		mClusterConfiguration = config;
		mMetadataServer = new MetadataServer();
		mDataServerMap = new HashMap<Integer, DataServer>();
		Map<Integer, DataserverConfiguration> configs = config.getAllDataserverConfigurations();
		for(int key : configs.keySet()) {
			mDataServerMap.put(key, new DataServer(configs.get(key)));
		}
	}
	
	public static DistributedFileSystem createInstance(JSONObject config) {
		if(sInstance == null) {
			sInstance = new DistributedFileSystem(ConfigUtils.getClusterConfig(config));
			return sInstance;
		} else {
			return null; // Singleton instance has already been created
		}
	}
	
	public static DistributedFileSystem getInstance() {
		return sInstance;
	}
	
	public boolean create(String filename, int client_id) {
		// TODO: Implement
		return true;
	}
	
	public boolean open(String filename, int client_id) {
		// TODO: Implement
		return true;
	}
	
	public long read(int fd, String buffer, long count, int client_id) {
		// TODO: Implement
		return 0;
	}
	
	public long written(int fd, String buffer, long count, int client_id) {
		// TODO: Implement
		return 0;
	}
	
	public long seek(int fd, long offset, int client_id) {
		// TODO: Implement
		return 0;
	}
	
	public boolean delete(int fd) {
		// TODO: Implement
		return true;
	}
	
	public FileAttribute stat(int fd) {
		// TODO: Implement
		return null;
	}
}