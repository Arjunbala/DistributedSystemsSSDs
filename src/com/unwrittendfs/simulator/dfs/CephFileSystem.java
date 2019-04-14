package com.unwrittendfs.simulator.dfs;

import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CephFileSystem  extends DistributedFileSystem  {

    // To choose replica
    private Map<Integer, List<DataServerTimeStamp>> locationVsServer;
    private Queue<DataServerTimeStamp> leastRecentlyUsed;

    protected CephFileSystem(ClusterConfiguration config, List<DataserverConfiguration> dataserverConfigurations) {
        super(config, dataserverConfigurations);
        locationVsServer = new HashMap<>();
        leastRecentlyUsed = new ConcurrentLinkedQueue<>();
        // Since rackId is not part of DS, Randomly assigning the rack Id uniformly to dataServer.
        // This can be changed and become  part of configuration as well
        // Every rack will have atleast 3 dataServers;
        int rackCount = dataserverConfigurations.size()/config.getNumberReplicas();
        int rackId = 1;
        Date curTime = new Date();
        for(DataserverConfiguration dataServer : dataserverConfigurations){
            locationVsServer.computeIfAbsent(rackId, k -> new ArrayList<>());
            DataServerTimeStamp serverTimeStamp = new DataServerTimeStamp(dataServer, curTime, rackId);
            locationVsServer.get(rackId).add(serverTimeStamp);
            if(++rackId > rackCount){
                rackId=1;
            }
            leastRecentlyUsed.add(serverTimeStamp);
        }
    }

    @Override
    protected List<DataServer> orderServersToRead(List<DataLocation> locations) {
        // In the case of CEPH, the prelica only reply when primary is not up or not in the cluster.
        // Since we are not simulating that, primary will always be there in the cluster.
        // orders Servers to read will always result in primary
        List<DataServer> dataServers = new ArrayList<DataServer>();
        for(DataLocation location : locations) {
            if(location.getRole().equals(DataLocation.DataRole.PRIMARY_REPLICA)){
                dataServers.add(mDataServerMap.get(location.getDataServer()));
                break;
            }

        }
        return dataServers;
    }

    // Measure the performance of this. This should be round robin
    @Override
    protected List<DataLocation> getLocationsForNewChunk() {
        List<DataLocation> dataLocations = new ArrayList<>();
        int replicas = mClusterConfiguration.getNumberReplicas();
        boolean isPrimaryAssigned = false;
        DataServerTimeStamp primaryDataServerTimeStamp = null;
        int maxAttemptToFindReplica = 3;
        for(int i = 0;i<replicas;i++){
            if(!isPrimaryAssigned){
                primaryDataServerTimeStamp = leastRecentlyUsed.remove();
                primaryDataServerTimeStamp.lastUsed = new Date();
                dataLocations.add(new DataLocation(primaryDataServerTimeStamp.dataserverConfiguration.getDataServerId(),
                        DataLocation.DataRole.PRIMARY_REPLICA));
                leastRecentlyUsed.add(primaryDataServerTimeStamp);
                isPrimaryAssigned = true;
            } else {
                int prevSize = dataLocations.size();
                DataServerTimeStamp dataServerTimeStamp = null;
                for(int j = 0;j < maxAttemptToFindReplica;j++){
                    dataServerTimeStamp = leastRecentlyUsed.remove();
                    if(dataServerTimeStamp.rackId != primaryDataServerTimeStamp.rackId){
                        dataLocations.add(new DataLocation(dataServerTimeStamp.dataserverConfiguration.getDataServerId(),
                                DataLocation.DataRole.SECONDARY_REPLICA));
                        dataServerTimeStamp.lastUsed = new Date();
                        leastRecentlyUsed.add(dataServerTimeStamp);
                        break;
                    } else {
                        leastRecentlyUsed.add(dataServerTimeStamp);
                    }
                }
                // We were not able to find a different rackID
                if(prevSize == dataLocations.size()){
                    dataLocations.add(new DataLocation(dataServerTimeStamp.dataserverConfiguration.getDataServerId(),
                            DataLocation.DataRole.SECONDARY_REPLICA));
                    dataServerTimeStamp.lastUsed = new Date();
                }
            }
        }
        return dataLocations;

    }

    private class DataServerTimeStamp{
        DataserverConfiguration dataserverConfiguration;
        Date lastUsed;
        int rackId;


        public DataServerTimeStamp(DataserverConfiguration dataserverConfiguration, Date lastUsed, int rackId) {
            this.dataserverConfiguration = dataserverConfiguration;
            this.lastUsed = lastUsed;
            this.rackId = rackId;
        }
    }

}
