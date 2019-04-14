package com.unwrittendfs.simulator.client;

import com.unwrittendfs.simulator.client.workload.Workload;
import lombok.Getter;
import lombok.Setter;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;


@Getter
@Setter
public class ClientWorkload {
    private Set<Client> Clients;
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

    public Set<Client> getClients() {
        return Clients;
    }

    public void setClients(Set<Client> clients) {
        Clients = clients;
    }
}
