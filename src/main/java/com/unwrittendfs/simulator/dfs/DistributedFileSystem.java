package com.unwrittendfs.simulator.dfs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.file.FileAttribute;
import com.unwrittendfs.simulator.utils.ConfigUtils;

public class DistributedFileSystem {
	
	private static DistributedFileSystem sInstance = null;
	private MetadataServer mMetadataServer; // Holds instance of Metadata server
	private ClusterConfiguration mClusterConfiguration; // Holds instance of cluster configuration
	private Map<Integer, DataServer> mDataServerMap; // List of available data-servers
	
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
	
	public int create(String filename, int client_id) {
		return mMetadataServer.createNewFile(filename, client_id);
	}
	
	public int open(String filename, int client_id) {
		return mMetadataServer.openFile(filename, client_id);
	}
	
	public boolean close(int fd, int client_id) {
		return mMetadataServer.closeFile(fd, client_id);
	}
	
	public long read(int fd, String buffer, long count, int client_id) {
		// TODO: Implement
		return 0;
	}
	
	public long write(int fd, String buffer, long count, int client_id) {
		// TODO: Implement
		return 0;
	}
	
	public long seek(int fd, long offset, int client_id) {
		return mMetadataServer.seekFile(fd, offset, client_id);
	}
	
	public boolean delete(int fd) {
		// First delete metadata from MDS and get list of data chunks to delete for each DS
		Map<Integer, List<Integer>> chunksToDelete = mMetadataServer.deleteFile(fd);
		if(chunksToDelete == null) {
			// Possibly file does not exist
			return false;
		}
		for(Integer ds : chunksToDelete.keySet()) {
			mDataServerMap.get(ds).deleteChunks(chunksToDelete.get(ds));
		}
		return true;
	}
	
	public FileAttribute stat(int fd) {
		return mMetadataServer.getFileAttributes(fd);
	}
}