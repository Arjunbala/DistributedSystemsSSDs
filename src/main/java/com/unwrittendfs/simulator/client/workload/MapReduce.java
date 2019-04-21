package com.unwrittendfs.simulator.client.workload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;

import java.util.*;

public class MapReduce implements IClientWorkload {

    private Integer mapperCount;

    // No. of partition
    private Integer reducerCount;
    private Integer skewness;
    private Integer iterationCount;
    private DistributedFileSystem mDfs;
    @JsonIgnore
    private Set<Integer> skewedPartition;
    @JsonIgnore
    private Random random;
    @JsonIgnore
    private int partitionCount;

    public MapReduce() {
    }

    public MapReduce(DistributedFileSystem mDfs) {
        this.mDfs = mDfs;
        this.random = new Random(5);
        this.skewedPartition = new HashSet<>();
        this.partitionCount = reducerCount;
    }


    //    private

    @Override
    public void execute() {
        selectRandomSkewedPartitions();


    }

    private void mapperWrite() {

    }

    private void mapperRead() {

    }

    private void reducerWrite() {

    }

    private void reducerRead() {

    }

    private void reducerDelete() {

    }

    private void selectRandomSkewedPartitions() {
        List<Integer> allPartitions = new ArrayList<>();
        for (int i = 0; i < reducerCount; i++) {
            allPartitions.add(i);
        }
        // TODO : This might result in non-replicable result. I don't think it's important. Let's check
        Collections.shuffle(allPartitions);
        for (int i = 0; i < skewness; i++) {
            int index = random.nextInt(allPartitions.size());
            skewedPartition.add(allPartitions.get(index));
            allPartitions.remove(index);
        }
    }

    public Integer getMapperCount() {
        return mapperCount;
    }

    public void setMapperCount(Integer mapperCount) {
        this.mapperCount = mapperCount;
    }

    public Integer getReducerCount() {
        return reducerCount;
    }

    public void setReducerCount(Integer reducerCount) {
        this.reducerCount = reducerCount;
    }

    public Integer getIterationCount() {
        return iterationCount;
    }

    public void setIterationCount(Integer iterationCount) {
        this.iterationCount = iterationCount;
    }
}
