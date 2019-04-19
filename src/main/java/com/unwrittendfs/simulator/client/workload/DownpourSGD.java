package com.unwrittendfs.simulator.client.workload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;
import com.unwrittendfs.simulator.utils.ConfigUtils;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class DownpourSGD implements IClientWorkload {


    private Long trainingDataSize;
    private Long sampleSize;
    private Integer clientCount;
    private Long batchSize;
    private Long iterationCount;


    @JsonIgnore
    private Long sampleCount;
    @JsonIgnore
    private Long samplePerClient;
    @JsonIgnore
    private DistributedFileSystem mDfs;

    public DownpourSGD(DistributedFileSystem mDfs) {
        this.mDfs = mDfs;
        DownpourSGD sgd = ConfigUtils.getSGDWorkloadConfig();
        this.trainingDataSize = sgd.trainingDataSize;
        this.sampleSize = sgd.sampleSize;
        this.clientCount = sgd.clientCount;
        this.batchSize = sgd.batchSize;
        this.iterationCount = sgd.iterationCount;
        this.sampleCount = this.trainingDataSize / sampleSize;
        this.samplePerClient = this.sampleCount / clientCount;
    }

    public DownpourSGD() {
    }

    class ClientRange {
        int id;
        Long start;
        Long end;

        @Override
        public String toString() {
            return "ClientRange{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }


    public void execute() {
        List<ClientRange> clients = assignWorkloadToTheClients();
        this.write(clients);
        for (long l = 0; l < iterationCount; l++) {
            Map<Integer, Set<Long>> clientVsMiniBatch = getMiniBatches(clients);
            this.readMiniBatches(clientVsMiniBatch);
        }
    }

    private void readMiniBatches(Map<Integer, Set<Long>> clientVsMiniBatch) {
        for (Integer clientId : clientVsMiniBatch.keySet()) {
            Set<Long> miniBatch = clientVsMiniBatch.get(clientId);
            for (Long sampleId : miniBatch) {
                int openfd = mDfs.open(String.valueOf(sampleId), clientId);
                if (openfd == -1) {
                    System.out.println("File failed to open for sample Id : " + sampleId);
                    return;
                }
                if (mDfs.read(openfd, "", sampleSize, clientId) <= 0) {
                    System.out.println("Failed to read for sample Id : " + sampleId);
                    return;
                }
                Simulation.incrementSimulatorTime();
                mDfs.close(openfd, clientId);
            }
        }
    }

    private Map<Integer, Set<Long>> getMiniBatches(List<ClientRange> clients) {
        Map<Integer, Set<Long>> clientVsMiniBatch = new HashMap<>();
        for (ClientRange client : clients) {
            Set<Long> miniBatch = new HashSet<>();
            while (miniBatch.size() <= batchSize) {
                miniBatch.add(ThreadLocalRandom.current().nextLong(client.start, client.end + 1));
            }
            clientVsMiniBatch.put(client.id, miniBatch);
        }
        return clientVsMiniBatch;
    }


    private void write(List<ClientRange> clients) {
        for (ClientRange client : clients) {
            for (Long start = client.start; start <= client.end; start++) {
                int openfd = mDfs.create(String.valueOf(start), client.id);
                if (openfd == -1) {
                    System.out.println("File failed to create for sample Id : " + start);
                    return;
                }
                if (mDfs.write(openfd, "", sampleSize, client.id) <= 0) {
                    System.out.println("Failed to write for sample Id : " + start);
                    return;
                }
                Simulation.incrementSimulatorTime();
                mDfs.close(openfd, client.id);
            }
        }
    }

    /*
    Workload starts with one.
    For ex. Client1 = [1,16]
    Client2 = [17,32]
    Client3 = [33,48]
    Client4 = [49,64]
     */
    private List<ClientRange> assignWorkloadToTheClients() {
        int clientId = 1;
        List<ClientRange> clients = new ArrayList<>();
        for (long l = 1; l < sampleCount; l = l + samplePerClient) {
            ClientRange client = new ClientRange();
            client.start = l;
            client.end = l + samplePerClient - 1;
            client.id = clientId++;
            clients.add(client);

        }
        return clients;
    }

    public Long getTrainingDataSize() {
        return trainingDataSize;
    }

    public void setTrainingDataSize(Long trainingDataSize) {
        this.trainingDataSize = trainingDataSize;
    }

    public Long getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(Long sampleSize) {
        this.sampleSize = sampleSize;
    }

    public Integer getClientCount() {
        return clientCount;
    }

    public void setClientCount(Integer clientCount) {
        this.clientCount = clientCount;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public Long getSampleCount() {
        if (sampleCount == null) {
            sampleCount = trainingDataSize / sampleSize;
        }
        return sampleCount;
    }

    public void setSampleCount(Long sampleCount) {
        this.sampleCount = sampleCount;
    }

    public Long getSamplePerClient() {
        if (samplePerClient == null) {
            samplePerClient = sampleCount / clientCount;
        }
        return samplePerClient;
    }

    public void setSamplePerClient(Long samplePerClient) {
        this.samplePerClient = samplePerClient;
    }

    public void setBatchSize(Long batchSize) {
        this.batchSize = batchSize;
    }

    public Long getIterationCount() {
        return iterationCount;
    }

    public void setIterationCount(Long iterationCount) {
        this.iterationCount = iterationCount;
    }

//    public static void main(String[] args) {
//        DownPourSGD test = new DownPourSGD();
//        test.execute();
//
//    }

}
