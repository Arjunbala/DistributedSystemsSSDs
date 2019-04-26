package com.unwrittendfs.simulator.dataserver;

import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dfs.cache.Cache;
import com.unwrittendfs.simulator.exceptions.GenericException;
import com.unwrittendfs.simulator.exceptions.PageCorruptedException;

import java.util.*;
import java.util.logging.Logger;


public class DataServer {

    public enum PageStatus {
        VALID, INVALID, FREE
    };

    // SSD Data Structures
    private Map<Integer, List<Long>> mChunkToPageMapping;
    private Map<Long, PageStatus> mPageList;
    private static Logger sLog;

    // Statistics Structures
    private Map<Long, Integer> mEraseMap;
    private Map<Long, Integer> mReadMap;
    private Cache cacheLayer;

    // Configuration Structures
    private DataserverConfiguration mConfig;
    private Random mRandomGenerator;

    public DataServer(DataserverConfiguration config) {
        mConfig = config;
        mChunkToPageMapping = new HashMap<>();
        mPageList = new HashMap<>();
        for (long i = 0; i < config.getTotalNumPages(); i++) {
            mPageList.put(i, PageStatus.FREE);
        }
        mEraseMap = new HashMap<>();
        for (long i = 0; i < config.getTotalNumPages(); i++) {
            mEraseMap.put(i, 0);
        }
        mReadMap = new HashMap<>();
        for (long i = 0; i < config.getTotalNumPages(); i++) {
            mReadMap.put(i, 0);
        }
        cacheLayer = new Cache(config.getCacheSize(), config.getPageSize());
        mRandomGenerator = new Random(mConfig.getmRandomSeed());
        sLog = Logger.getLogger(DataServer.class.getSimpleName());
        sLog.setLevel(Simulation.getLogLevel());
    }

    public long read(int chunk_id) throws PageCorruptedException, GenericException {

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
                    while (!canReadPageWithoutError(page)) {
                        retries++;
                        increment(mReadMap, page);
                        if (retries == mConfig.getMaxReadRetries()) {
                            increment(mReadMap, page);
                            sLog.info("No. of retries : " + retries);
                            throw new PageCorruptedException("The pageId: " + page + " in DS id : "
                                    + getConfig().getDataServerId() + " is corrupted and can't be read any longer");
                        }
                    }
                    System.out.println("Number retries: " + retries);
                    bytesRead += mConfig.getPageSize();
                    increment(mReadMap, page);
                    cacheLayer.add(page);
                    if (retries > mConfig.getMaxReadRetries() * mConfig.getmDataScrubbingThreshold()) {
                        pagesToMigrate.add(page);
                    }
                } else {
                    return -1;
                }
            }
        }
        if (!pagesToMigrate.isEmpty()) {
            System.out.println("Migrating chunk : " + chunk_id);
            handleDataScrubbing(chunk_id);
        }
        return bytesRead;
    }

    private void handleDataScrubbing(int chunk_id) throws GenericException {
        // In terms of wear, data scrubbing is equivalent of a write to this chunk
        write(chunk_id, mChunkToPageMapping.get(chunk_id).size() * mConfig.getmPageSize());
    }

    private boolean canReadPageWithoutError(long page) {
        double probability_error = (Math.pow(((double) mReadMap.get(page) / mConfig.getmMaxPageReadCount()),
                mConfig.getmDisturbanceReadsExponent()) +
                Math.pow(((double) mEraseMap.get(page) / mConfig.getMaxEraseCount()),
                        mConfig.getmDisturbanceCyclesExponent())) / 2;
//        System.out.println("Probability error of reading: " + probability_error);
        if (Double.compare(probability_error, 1.0) >= 0
                || mRandomGenerator.nextDouble() <= probability_error) {
            return false;
        }
        return true;
    }

    public long write(int chunk_id, long chunk_size) throws GenericException {
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
        if (Double.compare(getCurrentFreePageFraction(), getConfig().getmGCThreshold()) < 0) {
            // Trigger GC;
            triggerGC();
        }
        return (pagesAllocated.size())
                * mConfig.getPageSize();
    }

    private void triggerGC() {
        System.out.println("GC Triggered");
        boolean isDirtyBlock = false;
        for (long pgNo = 0; pgNo < mConfig.getTotalNumPages(); pgNo = pgNo + mConfig.getmPagesPerBlock()) {
            for (long start = pgNo; start < pgNo + mConfig.getmPagesPerBlock(); start++) {
                if (mPageList.get(start).equals(PageStatus.VALID)) {
                    isDirtyBlock = true;
                }
            }
//             If the block has no valid data then clean the block
            if (!isDirtyBlock) {
                for (long start = pgNo; start < pgNo + mConfig.getmPagesPerBlock(); start++) {
                    if (mPageList.get(start).equals(PageStatus.INVALID)) {
                        mPageList.put(start, PageStatus.FREE);
                        increment(mEraseMap, start);
                        mReadMap.put(start, 0);
                    }
                }
            }
        }

    }

    // Greedy Allocation policy while writing
    private List<Long> greedyPageAllocationPolicy(int numPages) throws GenericException {
        if (numPages == 0) {
            return new ArrayList<>();
        }
        int numBlocks = numPages/mConfig.getmPagesPerBlock();
        // Try to find out numPages which are least written to
        List<Long> allocatedPages = new ArrayList<>();

        // Sort in descending order of writes. The reason we are sorting in descending order because we want to fix our
        // heap size. If the maxWrite is greater in the heap is greater than the one during the iteration, we swap the pages.
        Queue<PagePEWrites> queue = new PriorityQueue<>((o1, o2) -> o2.write - o1.write);
        for (long pgNo = 0; pgNo < mConfig.getTotalNumPages(); pgNo=pgNo+mConfig.getPagesPerBlock()) {
            if (mPageList.get(pgNo).equals(PageStatus.FREE)) {
            	if (queue.size() < numBlocks) {
            		queue.add(new PagePEWrites(pgNo, mEraseMap.get(pgNo)));
                } else {
                	if (queue.peek().write > mEraseMap.get(pgNo)) {
                		queue.poll();
                        queue.add(new PagePEWrites(pgNo, mEraseMap.get(pgNo)));
                    }
                }
            }
        }
        if (queue.size() != numBlocks) {
            throw new GenericException("Required space is not available for allocation");
        }
        for (PagePEWrites peWrites : queue) {
        	for(long i=0;i<mConfig.getmPagesPerBlock();i++) {
        		allocatedPages.add(peWrites.pageNo+i);
                mPageList.put(peWrites.pageNo+i, PageStatus.VALID);
                cacheLayer.add(peWrites.pageNo+i);
        	}
        }
        return allocatedPages;
    }

    // TODO Not removing pages from the map???
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
            mChunkToPageMapping.remove(chunk_id);
        }
        // TODO : Are you sure don't want to trigger GC when free memeory goes below a threshold
        triggerGC();
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
        for (Long page : mPageList.keySet()) {
            if (mPageList.get(page) == PageStatus.FREE) {
                free++;
            } else if (mPageList.get(page) == PageStatus.INVALID) {
                invalid++;
            } else {
                valid++;
            }
        }
        int max_erase_count = 0;
        for(Long page : mEraseMap.keySet()) {
        	if(mEraseMap.get(page) > max_erase_count) {
        		max_erase_count = mEraseMap.get(page);
        	}
        }
        System.out.println("DS " + mConfig.getDataServerId() + " Max Erased: " + max_erase_count);
        System.out.println("DS" + mConfig.getDataServerId() + " Valid: " + valid
              + " Invalid: " + invalid + " Free: " + free);
        //System.out.println(mEraseMap.toString());
    }

    private void increment(Map<Long, Integer> map, long page) {
        int old_value = map.get(page);
        map.put(page, old_value + 1);
    }

    private double getCurrentFreePageFraction() {
        return (double) mPageList.keySet()
                .stream()
                .mapToLong(i -> i)
                .filter(i -> mPageList.get(i).equals(PageStatus.FREE))
                .count() / (double) mPageList.size();
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