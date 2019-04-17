package com.unwrittendfs.simulator.dfs;

import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.dfs.cache.Cache;

import java.util.*;
import java.util.logging.Logger;

// TODO : DiskUsage while allocating Data Servers
public class CephFileSystem extends DistributedFileSystem {

    // To choose replica

    private Queue<DataServerTimeStamp> minDiscUsageQueue;

    private static Logger sLog;

    protected CephFileSystem(ClusterConfiguration config, List<DataserverConfiguration> dataserverConfigurations) {
        super(config, dataserverConfigurations);

        minDiscUsageQueue = new PriorityQueue<>(new Comparator<DataServerTimeStamp>() {
            @Override
            public int compare(DataServerTimeStamp o1, DataServerTimeStamp o2) {
                return (int) (o1.diskUsage - o2.diskUsage);
            }
        });

        sLog = Logger.getLogger(Cache.class.getSimpleName());
        sLog.setLevel(Simulation.getLogLevel());
    }

    @Override
    protected List<DataServer> orderServersToRead(List<DataLocation> locations) {
        // In the case of CEPH, the prelica only reply when primary is not up or not in the cluster.
        // Since we are not simulating that, primary will always be there in the cluster.
        // orders Servers to read will always result in primary
        List<DataServer> dataServers = new ArrayList<DataServer>();
        List<DataServer> secondaryServers = new ArrayList<>();
        for (DataLocation location : locations) {
            if (location.getRole().equals(DataLocation.DataRole.PRIMARY_REPLICA)) {
                dataServers.add(mDataServerMap.get(location.getDataServer()));
            } else {
                secondaryServers.add(mDataServerMap.get(location.getDataServer()));
            }
        }
        Collections.shuffle(secondaryServers);
        dataServers.addAll(secondaryServers);
        return dataServers;
    }

    // Measure the performance of this. This should be round robin
    @Override
    protected List<DataLocation> getLocationsForNewChunk() {
        // Get the current set of data servers and it's disc usage
        for (Integer dataServerId : mDataServerMap.keySet()) {
            minDiscUsageQueue.add(new DataServerTimeStamp(mDataServerMap.get(dataServerId), mDataServerMap.get(dataServerId).getDiskUsage()));
        }
        List<DataLocation> dataLocations = new ArrayList<>();
        int replicas = mClusterConfiguration.getNumberReplicas();
        boolean isPrimaryAssigned = false;
        DataServerTimeStamp primaryDataServerTimeStamp = null;

        // TODO: what is the disc usage is beyond a threshold
        for (int i = 0; i < replicas; i++) {
            // Assign primary with the dataServer with least usage
            if (!isPrimaryAssigned) {
                primaryDataServerTimeStamp = minDiscUsageQueue.remove();
                dataLocations.add(new DataLocation(primaryDataServerTimeStamp.dataServer.getConfig().getDataServerId(),
                        DataLocation.DataRole.PRIMARY_REPLICA));
                isPrimaryAssigned = true;
            } else {
                // Assign Replica with the dataServers with next least usage
                primaryDataServerTimeStamp = minDiscUsageQueue.remove();
                dataLocations.add(new DataLocation(primaryDataServerTimeStamp.dataServer.getConfig().getDataServerId(),
                        DataLocation.DataRole.SECONDARY_REPLICA));
            }
        }
        sLog.info("Disc selected for write in Ceph FS :" + dataLocations);
        minDiscUsageQueue.clear();
        return dataLocations;

    }

    private class DataServerTimeStamp {
        DataServer dataServer;
        Long diskUsage;


        public DataServerTimeStamp(DataServer dataServer, Long diskUsage) {
            this.dataServer = dataServer;
            this.diskUsage = diskUsage;
        }
    }

}
