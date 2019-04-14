package com.unwrittendfs.simulator.dfs.cache;

import java.util.*;

public class Cache {
    private class CacheObject{
        private Long pageId;
        private Date lastTimeRead;


    }
    private Long cacheSize;
    private Map<Long, CacheObject> pageMap;
    private Queue<CacheObject> cacheQueue;
    private Long maxCountOfPages;

    public Cache(Long cacheSize, int pageSize){
        this.cacheSize = cacheSize;
        this.maxCountOfPages = cacheSize/pageSize;
        pageMap = new HashMap<>();
        cacheQueue = new PriorityQueue<>(Comparator.comparing(o -> o.lastTimeRead));
    }

    public void add(Long pageId){
        if(!pageMap.containsKey(pageId)){
            if(pageMap.size() ==  maxCountOfPages){
                evict();
            }
            CacheObject cacheObject = new CacheObject();
            cacheObject.lastTimeRead = new Date();
            cacheObject.pageId = pageId;
            cacheQueue.add(cacheObject);
            pageMap.put(pageId, cacheObject);
        }
    }

    public void evict(){
        if(cacheQueue.size() > 0) {
            CacheObject cacheToBeEvicted = cacheQueue.poll();
            pageMap.remove(cacheToBeEvicted.pageId);
        }
    }

    public boolean read(Long pageId){
        if(pageMap.containsKey(pageId)){
            pageMap.get(pageId).lastTimeRead = new Date();
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
