package com.unwrittendfs.simulator.dataserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataServer {
	
	public enum PageStatus {
		VALID,
		INVALID,
		FREE
	};
	
	// SSD Data Structures
	private Map<Integer, List<Long>> mChunkToPageMapping;
	private Map<Long, PageStatus> mPageList;
	
	// Statistics Structures
	private Map<Long, Integer> mEraseMap;
	private Map<Long, Integer> mReadMap;
	private Map<Long, Integer> mWriteMap;
	
	// Configuration Structures
	private DataserverConfiguration mConfig;
	
	public DataServer(DataserverConfiguration config) {
		mConfig = config;
		mChunkToPageMapping = new HashMap<Integer, List<Long>>();
		mPageList = new HashMap<Long, PageStatus>();
		for(long i=0;i<config.getTotalNumPages();i++) {
			mPageList.put(i, PageStatus.FREE);
		}
		mEraseMap = new HashMap<Long, Integer>();
		for(long i=0;i<config.getTotalNumPages();i++) {
			mEraseMap.put(i, 0);
		}
		mReadMap = new HashMap<Long, Integer>();
		for(long i=0;i<config.getTotalNumPages();i++) {
			mReadMap.put(i, 0);
		}
		mWriteMap = new HashMap<Long, Integer>();
		for(long i=0;i<config.getTotalNumPages();i++) {
			mWriteMap.put(i, 0);
		}
	}
	
	// TODO: Add caching logic
	public long read(int chunk_id) {
		List<Long> pagesToRead = mChunkToPageMapping.get(chunk_id);
		if(pagesToRead == null) {
			// Chunk does not exist in SSD
			return -1;
		}
		long bytesRead = 0;
		for(Long page : pagesToRead) {
			if(mPageList.get(page) == PageStatus.VALID) {
				// TODO: Implement retry logic based on an probabilistic error function
				bytesRead += mConfig.getPageSize();
				increment(mReadMap, page);
			}
		}
		return bytesRead;
	}
	
	// TODO: Do GC and then write if enough space is not available
	public long write(int chunk_id, long chunk_size) {
		int numPagesToAllocate = (int) (chunk_size/mConfig.getPageSize());
		int numBlocksToAllocate = numPagesToAllocate/mConfig.getPagesPerBlock();
		// Try to allocate whole contiguous blocks as far as possible
		List<Long> pagesAllocatedAsBlocks = allocateBlocks(numBlocksToAllocate);
		// For remaining pages, allocate them page wise
		int remainingPagesToAllocate = numPagesToAllocate - pagesAllocatedAsBlocks.size();
		List<Long> pagesAllocated = allocatePages(remainingPagesToAllocate);
		List<Long> oldPagesOfChunk = mChunkToPageMapping.get(chunk_id);
		if(oldPagesOfChunk != null) {
			// Means that chunk is being overwritten. Mark old pages as invalid
			for(Long page : oldPagesOfChunk) {
				mPageList.put(page, PageStatus.INVALID);
			}
		}
		// Update pages allocated to this chunk ID
		List<Long> allNewPagesAllocated = new ArrayList<Long>();
		allNewPagesAllocated.addAll(pagesAllocatedAsBlocks);
		allNewPagesAllocated.addAll(pagesAllocated);
		mChunkToPageMapping.put(chunk_id, allNewPagesAllocated);
		return (pagesAllocated.size()+pagesAllocatedAsBlocks.size())*mConfig.getPageSize();
	}
	
	private List<Long> allocateBlocks(int numBlocks) {
		long start = 0;
		List<Long> pages = new ArrayList<Long>();
		if(numBlocks == 0) {
			return pages; // Empty list
		}
		long end = mConfig.getTotalNumPages();
		while(start < end) {
			// Look from [start,start + pages_per_block) and check if all are free
			boolean all_pages_free = true;
			for(long i=start;i<start+mConfig.getPagesPerBlock();i++) {
				if(mPageList.get(i) != PageStatus.FREE) {
					all_pages_free = false;
					break;
				}
			}
			if(all_pages_free) {
				for(long i=start;i<start+mConfig.getPagesPerBlock();i++) {
					pages.add(i);
					mPageList.put(i, PageStatus.VALID);
				}
				numBlocks--;
				if(numBlocks == 0) {
					break;
				}
			}
			start += mConfig.getPagesPerBlock();
		}
		return pages;
	}
	
	private List<Long> allocatePages(int numPages) {
		List<Long> pages = new ArrayList<Long>();
		if(numPages == 0) {
			return pages; // empty list
		}
		long start = 0;
		long end = mConfig.getTotalNumPages();
		while(start < end) {
			// Allocate any page that is found to be free
			if(mPageList.get(start) == PageStatus.FREE) {
				pages.add(start);
				mPageList.put(start, PageStatus.VALID); // Mark page as in use
				numPages--;
				if(numPages == 0) {
					break;
				}
			}
			start++;
		}
		return pages;
	}
	
	public boolean deleteChunks(List<Integer> chunk_ids) {
		for(Integer chunk_id : chunk_ids) {
			List<Long> pagesToInvalidate = mChunkToPageMapping.get(chunk_id);
			if(pagesToInvalidate == null) {
				continue;
			}
			for(Long page : pagesToInvalidate) {
				if(mPageList.get(page) != PageStatus.FREE) {
					mPageList.put(page, PageStatus.INVALID);
				}
			}
		}
		return true;
	}
	
	private void increment(Map<Long, Integer> map, long page) {
		int old_value = map.get(page);
		map.put(page, old_value+1);
	}
}