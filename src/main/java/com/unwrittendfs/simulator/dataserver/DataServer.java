package com.unwrittendfs.simulator.dataserver;

import com.unwrittendfs.simulator.dfs.cache.Cache;


import java.util.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class DataServer {

	public enum PageStatus {
		VALID, INVALID, FREE
	};

	// SSD Data Structures
	private Map<Integer, List<Long>> mChunkToPageMapping;
	private Map<Long, PageStatus> mPageList;

	// Statistics Structures
	private Map<Long, Integer> mEraseMap;
	private Map<Long, Integer> mReadMap;
	private Map<Long, Integer> mWriteMap;
	private Cache cacheLayer;

	// Configuration Structures
	private DataserverConfiguration mConfig;
	private Random mRandomGenerator;

	public DataServer(DataserverConfiguration config) {
		mConfig = config;
		mChunkToPageMapping = new HashMap<Integer, List<Long>>();
		mPageList = new HashMap<Long, PageStatus>();
		for (long i = 0; i < config.getTotalNumPages(); i++) {
			mPageList.put(i, PageStatus.FREE);
		}
		mEraseMap = new HashMap<Long, Integer>();
		for (long i = 0; i < config.getTotalNumPages(); i++) {
			mEraseMap.put(i, 0);
		}
		mReadMap = new HashMap<Long, Integer>();
		for (long i = 0; i < config.getTotalNumPages(); i++) {
			mReadMap.put(i, 0);
		}
		mWriteMap = new HashMap<Long, Integer>();
		for (long i = 0; i < config.getTotalNumPages(); i++) {
			mWriteMap.put(i, 0);
		}
		cacheLayer = new Cache(config.getCacheSize(), config.getPageSize());
		mRandomGenerator = new Random(mConfig.getmRandomSeed());
	}

	public long read(int chunk_id) {

		List<Long> pagesToRead = mChunkToPageMapping.get(chunk_id);
		if (pagesToRead == null) {
			// Chunk does not exist in SSD
			return -1;
		}
		List<Long> pagesToMigrate = new ArrayList<>();
		long bytesRead = 0;
		for (Long page : pagesToRead) {
			if (cacheLayer.read(page)) {
				// Check if page is in cache. If not in cache, read from disk
				bytesRead += mConfig.getPageSize();
			} else {
				if (mPageList.get(page) == PageStatus.VALID) {
					int retries = 0;
					// model error on read
					if(!canReadPageWithoutError(page)) {
						retries++;
						if(retries == mConfig.getMaxReadRetries()) {
							increment(mReadMap, page);
							return -1; // read not successful, unrecoverable error
						}
					}
					System.out.println("Number retries: " + Integer.toString(retries));
					bytesRead += mConfig.getPageSize();
					increment(mReadMap, page);
					cacheLayer.add(page);
					if(retries > mConfig.getMaxReadRetries()*mConfig.getmDataScrubbingThreshold()) {
						pagesToMigrate.add(page);
					}
				} else {
					return -1;
				}
			}
		}
		if(!pagesToMigrate.isEmpty()) {
			handleDataScrubbing(chunk_id);
		}
		return bytesRead;
	}
	
	private void handleDataScrubbing(int chunk_id) {
		// In terms of wear, data scrubbing is equivalent of a write to this chunk
		write(chunk_id, mChunkToPageMapping.get(chunk_id).size()*mConfig.getmPageSize());
	}
	
	private boolean canReadPageWithoutError(long page) {
		double probability_error = (Math.pow((mReadMap.get(page)/mConfig.getMaxReadRetries()), 
				mConfig.getmDisturbanceReadsExponent()) + 
				Math.pow((mEraseMap.get(page)/mConfig.getMaxEraseCount()), 
						mConfig.getmDisturbanceCyclesExponent()))/2;
		System.out.println(probability_error);
		if(Double.compare(probability_error, 1.0) == 0 
				|| mRandomGenerator.nextDouble() <= probability_error) {
			return false;
		}
		return true;
	}

	public long write(int chunk_id, long chunk_size) {
		int numPagesToAllocate = (int) (chunk_size / mConfig.getPageSize());

		// Greedily select the pages according to the PE ratio
		List<Long> pagesAllocated = greedyPageAllocationPolicy(numPagesToAllocate);
		List<Long> oldPagesOfChunk = mChunkToPageMapping.get(chunk_id);
		// old chunks will exist if chunk is being overwritten
		if (oldPagesOfChunk != null) {
			// Means that chunk is being overwritten. Mark old pages as invalid
			for (Long page : oldPagesOfChunk) {
				mPageList.put(page, PageStatus.INVALID);
				// If I don't do it here then GC has to take care of removing it
				// from the cache
				cacheLayer.invalidateCache(page);
			}
		}

		mChunkToPageMapping.put(chunk_id, pagesAllocated);
		// If invalid page count goes above threshold, trigger GC
		if(Double.compare(getCurrentInvalidPageFraction(), getConfig().getmGCThreshold()) > 0){
			// Trigger GC;
			triggerGC();
		}
		return (pagesAllocated.size())
				* mConfig.getPageSize();
	}

	private void triggerGC(){
		for(long pgNo = 0; pgNo < mConfig.getTotalNumPages();pgNo++){
			if(mPageList.get(pgNo).equals(PageStatus.INVALID)){
				mPageList.put(pgNo, PageStatus.FREE);
				increment(mEraseMap,pgNo);
				mReadMap.put(pgNo, 0);
			}
		}
	}

	// Greedy Allocation policy while writing
	// TODO: Add checks if no. of blocks are allocated are sufficient
	private List<Long> greedyPageAllocationPolicy(int numPages){
		// Try to find out numPages which are least written to
		List<Long> allocatedPages = new ArrayList<>();
		
		// Sort in descending order of writes
		Queue<PagePEWrites> queue = new PriorityQueue<>(new Comparator<PagePEWrites>() {
			@Override
			public int compare(PagePEWrites o1, PagePEWrites o2) {
				return o2.write - o2.write;
			}
		});
		for(long pgNo = 0; pgNo < mConfig.getTotalNumPages();pgNo++){
			if(mPageList.get(pgNo).equals(PageStatus.FREE)){
				if(queue.size() < numPages){
					queue.add(new PagePEWrites(pgNo, mWriteMap.get(pgNo)));
				} else {
					if(queue.peek().write > mWriteMap.get(pgNo)){
						queue.poll();
						queue.add(new PagePEWrites(pgNo, mWriteMap.get(pgNo)));
					}
				}
			}
		}
		for (PagePEWrites peWrites : queue){
			allocatedPages.add(peWrites.pageNo);
			increment(mWriteMap, peWrites.pageNo);
			mPageList.put(peWrites.pageNo, PageStatus.VALID);
			cacheLayer.add(peWrites.pageNo);
		}
		return allocatedPages;
	}

	public boolean deleteChunks(List<Integer> chunk_ids) {
		for (Integer chunk_id : chunk_ids) {
			List<Long> pagesToInvalidate = mChunkToPageMapping.get(chunk_id);
			if (pagesToInvalidate == null) {
				continue;
			}
			for (Long page : pagesToInvalidate) {
				if (mPageList.get(page) != PageStatus.FREE) {
					mPageList.put(page, PageStatus.INVALID);
				}
				cacheLayer.invalidateCache(page);
			}
		}
		return true;
	}

	public DataserverConfiguration getConfig() {
		return mConfig;
	}

	public long getDiskUsage() {
		long pages = 0;
		for (Integer chunk : mChunkToPageMapping.keySet()) {
			pages += mChunkToPageMapping.get(chunk).size();
		}
		return pages * mConfig.getPageSize();
	}

	public void printStats() {
		// Print out number of free, invalid, valid pages
		int valid = 0;
		int invalid = 0;
		int free = 0;
		for(Long page : mPageList.keySet()) {
			if(mPageList.get(page) == PageStatus.FREE) {
				free++;
			} else if(mPageList.get(page) == PageStatus.INVALID) {
				invalid++;
			}
			else {
				valid++;
			}
		}
		System.out.println("DS" + Integer.toString(mConfig.getDataServerId()) + " Valid: " + Integer.toString(valid)
				+ " Invalid: " + Integer.toString(invalid) + " Free: " + Integer.toString(free));
	}

	private void increment(Map<Long, Integer> map, long page) {
		int old_value = map.get(page);
		map.put(page, old_value + 1);
	}

	private double getCurrentInvalidPageFraction(){
		return  (double) mPageList.keySet().stream().mapToLong(i -> i).filter(i -> mPageList.get(i).equals(PageStatus.INVALID)).count()/(double) mPageList.size();
	}

	class PagePEWrites {
		long pageNo;
		Integer write;

		public PagePEWrites(long pageNo, Integer write) {
			this.pageNo = pageNo;
			this.write = write;
		}
	}
}