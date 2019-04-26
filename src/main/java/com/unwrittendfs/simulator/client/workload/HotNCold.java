package com.unwrittendfs.simulator.client.workload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;
import com.unwrittendfs.simulator.exceptions.GenericException;
import com.unwrittendfs.simulator.utils.ConfigUtils;

import java.util.*;

public class HotNCold implements IClientWorkload {

    private Integer noOfFile;
    private Long iterationCount;
    private Integer skewedFileCount;
    // A skewness of 80 means skewedFile will be read or appended 80 out of 100. Other files will be accessed 20 out of 100
//    private Integer skewness;
    private Long fileSize;

    private Random random;
    private int userId = 0;

    @JsonIgnore
    private DistributedFileSystem dfs;

    @JsonIgnore
    private List<Integer> skewedFiles;

    @JsonIgnore
    private Map<Integer, Integer> nonSkewedFiles;


    public HotNCold() {
    }

    public HotNCold(DistributedFileSystem dfs, String workloadConfig) throws GenericException {
        this.dfs = dfs;
        this.random = new Random(5);
        this.nonSkewedFiles = new HashMap<>();
        this.skewedFiles = new ArrayList<>();
        HotNCold hotNCold = ConfigUtils.getHotNColdWorkloadConfig(workloadConfig);
        this.noOfFile = hotNCold.noOfFile;
        this.iterationCount = hotNCold.iterationCount;
        this.skewedFileCount = hotNCold.skewedFileCount;
        this.fileSize = hotNCold.fileSize;
    }

    @Override
    public void execute() throws GenericException {
        selectSkewedFiles();
        writeFiles();
        Random randomSkewedFile = new Random(5);
        Random randomNonSkewedFile = new Random(5);

        int skewedCount = 0;
        int nonskewedCount = 0;
        for (long l = 0; l != iterationCount; l++) {
            int randomNo = random.nextInt(100) + 1;
            // Below function takes care of dividing the workload 90:10
            // Read skewed file
            if (randomNo % 10 != 0) {
                System.out.println("skewedCount : " + ++skewedCount);
                int index = randomSkewedFile.nextInt(skewedFileCount);
                fileReadAndWrite(String.valueOf(skewedFiles.get(index)));

            } else {
                // Read non-skewed file.
                System.out.println("nonskewedCount : " + ++nonskewedCount);
                int index = randomNonSkewedFile.nextInt(nonSkewedFiles.size());
                fileReadAndWrite(String.valueOf(nonSkewedFiles.get(index)));

            }
        }

    }

    // Add a chunk, seek to 0th position and read the file
    private void fileReadAndWrite(String fileName) throws GenericException {
        int fd = this.dfs.open(fileName, userId);
        if (fd <= -1) {
            throw new GenericException("Failed to open file : " + fileName);
        }
        if (this.dfs.read(fd, "", dfs.stat(fd).getFileSize(), this.userId) <= 0) {
            throw new GenericException("Failed to read file : " + fileName);
        }
        // Hardcoding the write to be of a fixed
        if (this.dfs.write(fd, "", 1024, this.userId) <= 0) {
            throw new GenericException("Failed to write file : " + fileName);
        }
        dfs.close(fd, userId);
    }

    private void writeFiles() throws GenericException {
        for (int i = 0; i < noOfFile; i++) {
            int fd = this.dfs.create(String.valueOf(i), userId);
            if (fd <= -1) {
                throw new GenericException("Failed to create file : " + i);
            }
            if (this.dfs.write(fd, "", this.fileSize, this.userId) <= 0) {
                throw new GenericException("Failed to write file : " + i);
            }
            dfs.close(fd, userId);
            Simulation.incrementSimulatorTime();
        }
    }

    private void selectSkewedFiles() {
        Random rand = new Random(5);

        for (int i = 0; i < noOfFile; i++) {
            nonSkewedFiles.put(i, i);
        }

        for (int i = 0; i < skewedFileCount; i++) {
            int index = rand.nextInt(nonSkewedFiles.size());
            skewedFiles.add(nonSkewedFiles.get(index));
            nonSkewedFiles.put(index, nonSkewedFiles.get(nonSkewedFiles.size() - 1));
            nonSkewedFiles.remove(nonSkewedFiles.size() - 1);

        }
    }


    public Integer getNoOfFile() {
        return noOfFile;
    }

    public void setNoOfFile(Integer noOfFile) {
        this.noOfFile = noOfFile;
    }

    public Integer getSkewedFileCount() {
        return skewedFileCount;
    }

    public void setSkewedFileCount(Integer skewedFileCount) {
        this.skewedFileCount = skewedFileCount;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }

    public Long getIterationCount() {
        return iterationCount;
    }

    public void setIterationCount(Long iterationCount) {
        this.iterationCount = iterationCount;
    }
}
