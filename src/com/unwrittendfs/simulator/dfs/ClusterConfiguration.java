package com.unwrittendfs.simulator.dfs;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
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

	public long getmChunkSize() {
		return mChunkSize;
	}

	public void setmChunkSize(long mChunkSize) {
		this.mChunkSize = mChunkSize;
	}

	public int getmNumReplicas() {
		return mNumReplicas;
	}

	public void setmNumReplicas(int mNumReplicas) {
		this.mNumReplicas = mNumReplicas;
	}

	public double getmRecentCreationsFraction() {
		return mRecentCreationsFraction;
	}

	public void setmRecentCreationsFraction(double mRecentCreationsFraction) {
		this.mRecentCreationsFraction = mRecentCreationsFraction;
	}
}
