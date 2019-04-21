package com.unwrittendfs.simulator.client.workload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;
import com.unwrittendfs.simulator.exceptions.GenericException;
import com.unwrittendfs.simulator.exceptions.PageCorruptedException;
import com.unwrittendfs.simulator.utils.ConfigUtils;

import java.util.*;

public class DownpourSGD implements IClientWorkload {


    private Long trainingDataSize;
    private Long sampleSize;
    private Integer clientCount;
    private Long batchSize;
    private Long iterationCount;

    @JsonIgnore
    private int testFileCreatorClientId;
    @JsonIgnore
    private Long sampleCount;
    @JsonIgnore
    private Long samplePerClient;
    @JsonIgnore
    private DistributedFileSystem mDfs;
    @JsonIgnore
    private String trainingFileName = "trainingData";
    @JsonIgnore
    private Integer randomSeed = 5;
    @JsonIgnore
    private Random rand;


    public DownpourSGD(DistributedFileSystem mDfs) throws GenericException {
        this.mDfs = mDfs;
        DownpourSGD sgd = ConfigUtils.getSGDWorkloadConfig();
        this.trainingDataSize = sgd.trainingDataSize;
        this.sampleSize = sgd.sampleSize;
        this.clientCount = sgd.clientCount;
        this.batchSize = sgd.batchSize;
        this.iterationCount = sgd.iterationCount;
        this.sampleCount = this.trainingDataSize / sampleSize;
        this.samplePerClient = this.sampleCount / clientCount;
        this.testFileCreatorClientId = 0;
        rand = new Random(this.randomSeed);
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


    public void execute() throws GenericException, PageCorruptedException {
        this.write();
        List<ClientRange> clients = assignWorkloadToTheClients();
        for (long l = 0; l < iterationCount; l++) {
            Map<Integer, List<Long>> clientVsMiniBatch = getMiniBatches(clients);
            this.readMiniBatches(clientVsMiniBatch);

        }
    }

    // Read the batches
    private void readMiniBatches(Map<Integer, List<Long>> clientVsMiniBatch) throws GenericException, PageCorruptedException {

        // Open file for each client;
        Map<Integer, Integer> clientVsFd = new HashMap<>();
        for (int client = 1; client <= clientCount; client++) {
            int openFd = mDfs.open(this.trainingFileName, client);
            if (openFd <= -1) {
                throw new GenericException("File failed to open for Client Id : " + client);
            }
            clientVsFd.put(client, openFd);
        }
        Simulation.incrementSimulatorTime();

        // Seek and Read files
        for (int i = 0; i < this.batchSize; i++) {
            for (int client = 1; client <= clientCount; client++) {
                Long sampleId = clientVsMiniBatch.get(client).get(i);
                mDfs.seek(clientVsFd.get(client), (sampleId - 1) * sampleSize, client);
                mDfs.read(clientVsFd.get(client), "", sampleSize, client);
            }
            Simulation.incrementSimulatorTime();
        }

        // Close for each client
        for (int client = 1; client <= clientCount; client++) {
            mDfs.close(clientVsFd.get(client), client);
        }
        Simulation.incrementSimulatorTime();


    }

    // Dividing the samples into unique set of mini batches. Uniqueness could be an issue here as it might get stuck
    // into a infinite loop. This will work if batchSize is much smaller than the actual assignment of the workload
    private Map<Integer, List<Long>> getMiniBatches(List<ClientRange> clients) {
        Map<Integer, List<Long>> clientVsMiniBatch = new HashMap<>();
        for (ClientRange client : clients) {
            List<Long> miniBatch = new ArrayList<>();
            while (miniBatch.size() < batchSize) {
                int randomNumber = rand.nextInt(this.samplePerClient.intValue()) + 1;
                miniBatch.add(randomNumber + (client.id - 1) * this.samplePerClient);
            }
            clientVsMiniBatch.put(client.id, miniBatch);
        }
        return clientVsMiniBatch;
    }


    private void write() throws GenericException {

        int fd = this.mDfs.create(this.trainingFileName, this.testFileCreatorClientId);
        if (fd <= -1) {
            throw new GenericException("Failed to write test file");
        }
        this.mDfs.write(fd, "", this.trainingDataSize, this.testFileCreatorClientId);
        Simulation.incrementSimulatorTime();
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
