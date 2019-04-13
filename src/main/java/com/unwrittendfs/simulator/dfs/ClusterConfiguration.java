package com.unwrittendfs.simulator.dfs;

import java.util.Map;

import org.json.simple.JSONObject;

import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;

public class ClusterConfiguration {
	
	private long mChunkSize;
	private int mNumReplicas;
	private double mRecentCreationsFraction;
	private Map<Integer, DataserverConfiguration> mDataserverConfigs;
	
	public ClusterConfiguration(JSONObject config) {
		// TODO: Parse config and initialize member variables
	}
	
	public long getChunkSize() {
		return mChunkSize;
	}
	
	public int getNumberReplicas() {
		return mNumReplicas;
	}
	
	public double getRecentCreationsFraction() {
		return mRecentCreationsFraction;
	}
	
	public DataserverConfiguration getDataserverConfiguration(int id) {
		return mDataserverConfigs.get(id);
	}
	
	public Map<Integer, DataserverConfiguration> getAllDataserverConfigurations() {
		return mDataserverConfigs;
	}
}