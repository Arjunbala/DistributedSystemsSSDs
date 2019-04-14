package com.unwrittendfs.simulator.dfs;

public class ClusterConfiguration {
	
	private long mChunkSize;
	private long mNumReplicas;


	public long getChunkSize() {
		return mChunkSize;
	}
	
	public long getNumberReplicas() {
		return mNumReplicas;
	}

	public void setmChunkSize(long mChunkSize) {
		this.mChunkSize = mChunkSize;
	}

	public void setmNumReplicas(long mNumReplicas) {
		this.mNumReplicas = mNumReplicas;
	}
}