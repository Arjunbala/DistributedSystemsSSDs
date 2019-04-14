package com.unwrittendfs.simulator.dfs;

import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.dfs.cache.Cache;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

// TODO : DiskUsage while allocating Data Servers
public class CephFileSystem  extends DistributedFileSystem  {

    // To choose replica
    private Map<Integer, List<DataServerTimeStamp>> locationVsServer;
    private Queue<DataServerTimeStamp> leastRecentlyUsedServers;

    private static Logger sLog;

    protected CephFileSystem(ClusterConfiguration config, List<DataserverConfiguration> dataserverConfigurations) {
        super(config, dataserverConfigurations);
        locationVsServer = new HashMap<>();
        leastRecentlyUsedServers = new ConcurrentLinkedQueue<>();
        // Since rackId is not part of DS, Randomly assigning the rack Id uniformly to dataServer.
        // This can be changed and become  part of configuration
        // Every rack will have atleast 3 dataServers;
        int rackCount = config.getNumberReplicas();
        int rackId = 1;
        for(DataserverConfiguration dataServer : dataserverConfigurations){
            locationVsServer.computeIfAbsent(rackId, k -> new ArrayList<>());
            DataServerTimeStamp serverTimeStamp = new DataServerTimeStamp(dataServer, rackId);
            locationVsServer.get(rackId).add(serverTimeStamp);
            if(++rackId > rackCount){
                rackId=1;
            }
            leastRecentlyUsedServers.add(serverTimeStamp);
        }
        sLog = Logger.getLogger(Cache.class.getSimpleName());
        sLog.setLevel(Simulation.getLogLevel());
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
                primaryDataServerTimeStamp = leastRecentlyUsedServers.remove();
                dataLocations.add(new DataLocation(primaryDataServerTimeStamp.dataserverConfiguration.getDataServerId(),
                        DataLocation.DataRole.PRIMARY_REPLICA));
                leastRecentlyUsedServers.add(primaryDataServerTimeStamp);
                isPrimaryAssigned = true;
            } else {
                int prevSize = dataLocations.size();
                DataServerTimeStamp dataServerTimeStamp = null;
                for(int j = 0;j < maxAttemptToFindReplica;j++){
                    dataServerTimeStamp = leastRecentlyUsedServers.remove();
                    if(dataServerTimeStamp.rackId != primaryDataServerTimeStamp.rackId){
                        dataLocations.add(new DataLocation(dataServerTimeStamp.dataserverConfiguration.getDataServerId(),
                                DataLocation.DataRole.SECONDARY_REPLICA));

                        leastRecentlyUsedServers.add(dataServerTimeStamp);
                        break;
                    } else {
                        sLog.info("Attempting reassignment of replica as the rackId mismatched with the primary");
                        leastRecentlyUsedServers.add(dataServerTimeStamp);
                    }
                }
                // We were not able to find a different rackID
                if(prevSize == dataLocations.size()){
                    sLog.info("Can't find different rack for primary and secondary therefore assigning in the same");
                    dataLocations.add(new DataLocation(dataServerTimeStamp.dataserverConfiguration.getDataServerId(),
                            DataLocation.DataRole.SECONDARY_REPLICA));
                }
            }
        }
        return dataLocations;

    }

    private class DataServerTimeStamp{
        DataserverConfiguration dataserverConfiguration;
        int rackId;


        public DataServerTimeStamp(DataserverConfiguration dataserverConfiguration, int rackId) {
            this.dataserverConfiguration = dataserverConfiguration;
            this.rackId = rackId;
        }
    }

}
