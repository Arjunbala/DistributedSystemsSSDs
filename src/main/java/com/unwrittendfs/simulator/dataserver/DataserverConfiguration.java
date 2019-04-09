package com.unwrittendfs.simulator.dataserver;

import java.util.Random;

import org.json.simple.JSONObject;

public class DataserverConfiguration {
	
	private int mPageSize;
	private int mPagesPerBlock;
	private long mTotalPages;
	private int mMaxReadRetries;
	private int mRandomSeed;
	private Random mRand;
	private int mMaxPageEraseCount;
	
	public DataserverConfiguration(JSONObject config) {
		// TODO: Initialize members from config
		mRand = new Random(mRandomSeed);
	}
	
	public int getPageSize() {
		return mPageSize;
	}
	
	public int getPagesPerBlock() {
		return mPagesPerBlock;
	}
	
	public long getTotalNumPages() {
		return mTotalPages;
	}
	
	public int getMaxReadRetries() {
		return mMaxReadRetries;
	}
	
	public int getRandomNumber() {
		return mRand.nextInt() % 100;
	}
	
	public int getMaxEraseCount() {
		return mMaxPageEraseCount;
	}
}