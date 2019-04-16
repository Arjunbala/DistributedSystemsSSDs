package com.unwrittendfs.simulator.dfs.cache;

import com.unwrittendfs.simulator.Simulation;

import java.util.*;
import java.util.logging.Logger;

// TODO :Add statistic for Cache
public class Cache {

    private class CacheObject{
        private Long pageId;
        private Long lastTimeRead;
    }

    private static Logger sLog;
    private Long cacheSize;
    private Map<Long, CacheObject> pageMap;
    private Queue<CacheObject> cacheQueue;
    private Long maxCountOfPages;

    public Cache(Long cacheSize, int pageSize){
        this.cacheSize = cacheSize;
        this.maxCountOfPages = cacheSize/pageSize;
        pageMap = new HashMap<>();
        cacheQueue = new PriorityQueue<>(Comparator.comparing(o -> o.lastTimeRead));
        sLog = Logger.getLogger(Cache.class.getSimpleName());
        sLog.setLevel(Simulation.getLogLevel());
    }

    public void add(Long pageId){
        if(!pageMap.containsKey(pageId)){
            if(pageMap.size() ==  maxCountOfPages){
                evict();
            }
//            sLog.info("Page Added in the cache: " + pageId);
            if(pageMap.size() < maxCountOfPages) {
                CacheObject cacheObject = new CacheObject();
                cacheObject.lastTimeRead = Simulation.getSimulatorTime();
                cacheObject.pageId = pageId;
                cacheQueue.add(cacheObject);
                pageMap.put(pageId, cacheObject);
            }
        }
    }

    public void evict(){
        if(cacheQueue.size() > 0) {
            CacheObject cacheToBeEvicted = cacheQueue.poll();
            pageMap.remove(cacheToBeEvicted.pageId);
//            sLog.info("Page Evicted in the cache: " + cacheToBeEvicted.pageId);
        }
    }

    public boolean read(Long pageId){
        if(pageMap.containsKey(pageId)){
            sLog.info("Page served from cache: " + pageId);
            pageMap.get(pageId).lastTimeRead = Simulation.getSimulatorTime();
            return true;
        } else {
            return false;
        }
    }

    public void invalidateCache(Long pageId){
        if(pageMap.containsKey(pageId)){
            CacheObject cacheObject = pageMap.get(pageId);
            cacheQueue.remove(cacheObject);
            pageMap.remove(pageId);
//            sLog.info("Page being invalidated in the cache: " + pageId);
        }
    }

    public Long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(Long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public Map<Long, CacheObject> getPageMap() {
        return pageMap;
    }

    public void setPageMap(Map<Long, CacheObject> pageMap) {
        this.pageMap = pageMap;
    }

    public Queue<CacheObject> getCacheQueue() {
        return cacheQueue;
    }

    public void setCacheQueue(Queue<CacheObject> cacheQueue) {
        this.cacheQueue = cacheQueue;
    }
}
