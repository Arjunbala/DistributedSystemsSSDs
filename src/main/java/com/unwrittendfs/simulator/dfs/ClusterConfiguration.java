package com.unwrittendfs.simulator.dfs;

public class ClusterConfiguration {
	
	private long mChunkSize;
	private int mNumReplicas;
	private double mRecentCreationsFraction;
	
	public ClusterConfiguration() {
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
}
