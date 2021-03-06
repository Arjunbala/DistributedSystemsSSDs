package com.unwrittendfs.simulator.dataserver;

import java.util.Random;

public class DataserverConfiguration {
	
	private int mPageSize;
	private int mPagesPerBlock;
	private long mTotalPages;
	private int mMaxReadRetries;
	private int mRandomSeed;
	private Random mRand;
	private int mMaxPageEraseCount;
	private int dataServerId;
	private Long cacheSize;
	private int mMaxPageReadCount;

	private Double mGCThreshold;

	private double mDisturbanceCyclesExponent;
	private double mDisturbanceReadsExponent;
	private double mDataScrubbingThreshold;

	public double getmDataScrubbingThreshold() {
		return mDataScrubbingThreshold;
	}

	public void setmDataScrubbingThreshold(double mDataScrubbingThreshold) {
		this.mDataScrubbingThreshold = mDataScrubbingThreshold;
	}

	public double getmDisturbanceCyclesExponent() {
		return mDisturbanceCyclesExponent;
	}

	public void setmDisturbanceCyclesExponent(double mDisturbanceCyclesExponent) {
		this.mDisturbanceCyclesExponent = mDisturbanceCyclesExponent;
	}

	public double getmDisturbanceReadsExponent() {
		return mDisturbanceReadsExponent;
	}

	public void setmDisturbanceReadsExponent(double mDisturbanceReadsExponent) {
		this.mDisturbanceReadsExponent = mDisturbanceReadsExponent;
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

	public int getmPageSize() {
		return mPageSize;
	}

	public void setmPageSize(int mPageSize) {
		this.mPageSize = mPageSize;
	}

	public int getmPagesPerBlock() {
		return mPagesPerBlock;
	}

	public void setmPagesPerBlock(int mPagesPerBlock) {
		this.mPagesPerBlock = mPagesPerBlock;
	}

	public long getmTotalPages() {
		return mTotalPages;
	}

	public void setmTotalPages(long mTotalPages) {
		this.mTotalPages = mTotalPages;
	}

	public int getmMaxReadRetries() {
		return mMaxReadRetries;
	}

	public void setmMaxReadRetries(int mMaxReadRetries) {
		this.mMaxReadRetries = mMaxReadRetries;
	}

	public int getmRandomSeed() {
		return mRandomSeed;
	}

	public void setmRandomSeed(int mRandomSeed) {
		this.mRandomSeed = mRandomSeed;
	}

	public Random getmRand() {
		return mRand;
	}

	public void setmRand(Random mRand) {
		this.mRand = mRand;
	}

	public int getmMaxPageEraseCount() {
		return mMaxPageEraseCount;
	}

	public void setmMaxPageEraseCount(int mMaxPageEraseCount) {
		this.mMaxPageEraseCount = mMaxPageEraseCount;
	}

	public int getDataServerId() {
		return dataServerId;
	}

	public void setDataServerId(int dataServerId) {
		this.dataServerId = dataServerId;
	}

	public Long getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(Long cacheSize) {
		this.cacheSize = cacheSize;
	}

	public Double getmGCThreshold() {
		return mGCThreshold;
	}

	public void setmGCThreshold(Double mGCThreshold) {
		this.mGCThreshold = mGCThreshold;
	}

	public int getmMaxPageReadCount() {
		return mMaxPageReadCount;
	}

	public void setmMaxPageReadCount(int mMaxPageReadCount) {
		this.mMaxPageReadCount = mMaxPageReadCount;
	}

	@Override
	public String toString() {
		return "DataserverConfiguration{" +
				"mPageSize=" + mPageSize +
				", mPagesPerBlock=" + mPagesPerBlock +
				", mTotalPages=" + mTotalPages +
				", mMaxReadRetries=" + mMaxReadRetries +
				", mRandomSeed=" + mRandomSeed +
				", mRand=" + mRand +
				", mMaxPageEraseCount=" + mMaxPageEraseCount +
				", dataServerId=" + dataServerId +
				", cacheSize=" + cacheSize +
				'}';
	}
}