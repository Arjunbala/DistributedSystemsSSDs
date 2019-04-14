package com.unwrittendfs.simulator.client;

import com.unwrittendfs.simulator.client.workload.Workload;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;



public class ClientWorkload {
    private Set<Integer> ClientId;
    private Queue<Workload> workloadQueue;

    private Workload getWorkloadFromQueue(){
        if(workloadQueue != null && !workloadQueue.isEmpty()){
            return workloadQueue.poll();
        } else {
            return null;
        }
    }

    private void addWorkloadToQueue(Workload workload){
        if(workloadQueue == null){
            workloadQueue = new ConcurrentLinkedQueue<>();
        }
        workloadQueue.add(workload);
    }



    public Queue<Workload> getWorkloadQueue() {
        return workloadQueue;
    }

    public void setWorkloadQueue(Queue<Workload> workloadQueue) {
        this.workloadQueue = workloadQueue;
    }

    public Set<Integer> getClientId() {
        return ClientId;
    }

    public void setClientId(Set<Integer> clientId) {
        ClientId = clientId;
    }
}
