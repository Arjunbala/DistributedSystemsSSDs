package com.unwrittendfs.simulator.client.workload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;
import com.unwrittendfs.simulator.exceptions.GenericException;
import com.unwrittendfs.simulator.utils.ConfigUtils;

import java.util.*;

public class MapReduce implements IClientWorkload {

    // No. of partition
    private Integer reducerCount;
    private Integer dataSize;
    private Integer skewness;
    private DistributedFileSystem dfs;
    @JsonIgnore
    private Set<Integer> skewedPartition;
    @JsonIgnore
    private Random random;
    @JsonIgnore
    private int partitionCount;
    @JsonIgnore
    private int userId = 0;

    public MapReduce() {
    }

    public MapReduce(DistributedFileSystem dfs) throws GenericException {
        MapReduce mapReduce = ConfigUtils.getMapReduceWorkloadConfig();
        this.dfs = dfs;
        this.reducerCount = mapReduce.reducerCount;
        this.dataSize = mapReduce.dataSize;
        this.skewness = mapReduce.skewness;
        this.random = new Random(5);
        this.skewedPartition = new HashSet<>();
        this.partitionCount = reducerCount;
    }

    @Override
    public void execute() throws GenericException {
        selectRandomSkewedPartitions();
        int iteration = 0;
        mapperWrite(iteration);
        while (true) {
            iteration++;
            reducerRead(iteration);
            reducerWrite(iteration);
            mapperRead(iteration);
            reducerDelete(iteration);
        }
    }


    private void mapperWrite(int iterationNumber) throws GenericException {
        for (int i = 0; i < partitionCount; i++) {
            String fileName = iterationNumber + "_" + i;
            int fd = this.dfs.create(fileName, userId);
            if (fd <= -1) {
                throw new GenericException("Failed to create file : " + fileName);
            }
            if (skewedPartition.contains(i)) {
                if (this.dfs.write(fd, "", skewness * this.dataSize, 0) <= 0) {
                    throw new GenericException("Failed to write file : " + fileName);
                }
            } else {
                if (this.dfs.write(fd, "", this.dataSize, 0) <= 0) {
                    throw new GenericException("Failed to write file : " + fileName);
                }
            }
            this.dfs.close(fd, userId);
        }
        Simulation.incrementSimulatorTime();
    }

    private void mapperRead(int iterationNumber) throws GenericException {
        for (int i = 0; i < partitionCount; i++) {
            String fileName = (iterationNumber) + "_" + i;
            int fd = this.dfs.open(fileName, userId);
            if (fd <= -1) {
                throw new GenericException("Failed to create file : " + fileName);
            }
            if (this.dfs.read(fd, "", dfs.stat(fd).getFileSize(), userId) <= 0) {
                throw new GenericException("Failed to read file : " + fileName);
            }
            this.dfs.close(fd, userId);
        }
        Simulation.incrementSimulatorTime();
    }

    private void reducerWrite(int iterationNumber) throws GenericException {
        for (int i = 0; i < partitionCount; i++) {
            String fileName = iterationNumber + "_" + i;
            int fd = this.dfs.create(fileName, userId);
            if (fd <= -1) {
                throw new GenericException("Failed to create file : " + fileName);
            }
            if (skewedPartition.contains(i)) {
                if (this.dfs.write(fd, "", skewness * this.dataSize, 0) <= 0) {
                    throw new GenericException("Failed to write file : " + fileName);
                }
            } else {
                if (this.dfs.write(fd, "", this.dataSize, 0) <= 0) {
                    throw new GenericException("Failed to write file : " + fileName);
                }
            }
            this.dfs.close(fd, userId);
        }
        Simulation.incrementSimulatorTime();
    }

    private void reducerRead(int iterationNumber) throws GenericException {
        for (int i = 0; i < partitionCount; i++) {
            String fileName = (iterationNumber - 1) + "_" + i;
            int fd = this.dfs.open(fileName, userId);
            if (fd <= -1) {
                throw new GenericException("Failed to create file : " + fileName);
            }
            if (this.dfs.read(fd, "", dfs.stat(fd).getFileSize(), userId) <= 0) {
                throw new GenericException("Failed to read file : " + fileName);
            }
            this.dfs.close(fd, userId);
        }
        Simulation.incrementSimulatorTime();

    }

    private void reducerDelete(int iterationNumber) throws GenericException {
        for (int i = 0; i < partitionCount; i++) {
            String fileName = (iterationNumber - 1) + "_" + i;
            int fd = this.dfs.open(fileName, userId);
            if (fd <= -1) {
                throw new GenericException("Failed to create file : " + fileName);
            }
            if (!this.dfs.delete(fd)) {
                throw new GenericException("Failed to delete file : " + fileName);
            }
        }
        Simulation.incrementSimulatorTime();
    }

    private void selectRandomSkewedPartitions() {
        List<Integer> allPartitions = new ArrayList<>();
        for (int i = 0; i < reducerCount; i++) {
            allPartitions.add(i);
        }
        // TODO : This might result in non-replicable result. I don't think it's important. Let's check
        Collections.shuffle(allPartitions);
        for (int i = 0; i < 3; i++) {
            int index = random.nextInt(allPartitions.size());
            skewedPartition.add(allPartitions.get(index));
            allPartitions.remove(index);
        }
    }

    public Integer getReducerCount() {
        return reducerCount;
    }

    public void setReducerCount(Integer reducerCount) {
        this.reducerCount = reducerCount;
    }

    public Integer getDataSize() {
        return dataSize;
    }

    public void setDataSize(Integer dataSize) {
        this.dataSize = dataSize;
    }

    public Integer getSkewness() {
        return skewness;
    }

    public void setSkewness(Integer skewness) {
        this.skewness = skewness;
    }
}
