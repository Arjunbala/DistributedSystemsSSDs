package com.unwrittendfs.simulator.dfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;

public class GoogleFileSystem extends DistributedFileSystem {
	
	private Map<Integer, Long> mDataServerLastCreateMap;
	public GoogleFileSystem(ClusterConfiguration config, List<DataserverConfiguration> dataserverConfigs) {
		super(config, dataserverConfigs);
		mDataServerLastCreateMap = new HashMap<Integer, Long>();
		for(Integer dataserver : mDataServerMap.keySet()) {
			mDataServerLastCreateMap.put(dataserver, (long) 0);
		}
	}
	
	@Override
	protected List<DataServer> orderServersToRead(List<DataLocation> locations) {
		// Generate a random permutation of the servers and return to the client
		List<DataServer> dataServers = new ArrayList<DataServer>();
		for(DataLocation location : locations) {
			dataServers.add(mDataServerMap.get(location.getDataServer()));
		}
		// Shuffle the data servers for randomness
		Collections.shuffle(dataServers);
		return dataServers;
	}
	
	@Override
	protected List<DataLocation> getLocationsForNewChunk() {
		// Arrange servers by timestamp
		List<DataServerTimestamp> servers = new ArrayList<DataServerTimestamp>();
		for(Integer server : mDataServerLastCreateMap.keySet()) {
			servers.add(new DataServerTimestamp(server, mDataServerLastCreateMap.get(server)));
		}
		// Sort the servers by timestamp at which last chunk was created
		Collections.sort(servers, new DataServerTimestampComparator());
		double createdFraction = mClusterConfiguration.getRecentCreationsFraction();
		// Pick servers to consider
		int number_servers_to_consider = (int) Math.max((int)createdFraction*servers.size(), 
				mClusterConfiguration.getNumberReplicas());
		servers = servers.subList(0, number_servers_to_consider);
		// From these list of servers, need to prioritize by disk utilization
		List<DataServerUtilization> serverUtilizations = new ArrayList<DataServerUtilization>();
		for(DataServerTimestamp dataserver : servers) {
			serverUtilizations.add(new DataServerUtilization(dataserver.getDataServer(), 
					mDataServerMap.get(dataserver.getDataServer()).getDiskUsage()));
		}
		// Sort by disk usage
		Collections.sort(serverUtilizations, new DataServerUtilizationComparator());
		// We now need to consider the servers to suit replicas.
		serverUtilizations = serverUtilizations.subList(0, mClusterConfiguration.getNumberReplicas());
		List<DataLocation> locations = new ArrayList<DataLocation>(); // final list of locations
		boolean isPrimaryAssigned = false; // one primary ; rest are secondaries
		for(DataServerUtilization server : serverUtilizations) {
			if(isPrimaryAssigned) {
				locations.add(new DataLocation(server.getDataServer(), DataLocation.DataRole.SECONDARY_REPLICA));
			} else {
				locations.add(new DataLocation(server.getDataServer(), DataLocation.DataRole.PRIMARY_REPLICA));
				isPrimaryAssigned = true;
			}
		}
		return locations;
	}
	
	private class DataServerTimestamp {
		private Integer mDataServerId;
		private Long mTimestamp;
		
		public DataServerTimestamp(Integer id, Long timestamp) {
			mDataServerId = id;
			mTimestamp = timestamp;
		}
		
		public int getDataServer() {
			return mDataServerId;
		}
		
		public long getTimestamp() {
			return mTimestamp;
		}
	}
	
	private class DataServerUtilization {
		private Integer mDataServerId;
		private Long mDiskUtilization;
		
		public DataServerUtilization(Integer id, Long utilization) {
			mDataServerId = id;
			mDiskUtilization = utilization;
		}
		
		public int getDataServer() {
			return mDataServerId;
		}
		
		public long getDiskUtilization() {
			return mDiskUtilization;
		}
	}
	
	private class DataServerTimestampComparator implements Comparator<DataServerTimestamp> {

		@Override
		public int compare(DataServerTimestamp arg0, DataServerTimestamp arg1) {
			if(arg0.getTimestamp() < arg1.getTimestamp()) {
				return -1;
			} else if(arg0.getTimestamp() > arg1.getTimestamp()){
				return 1;
			}
			return arg0.getDataServer() - arg1.getDataServer();
		}
		
	}
	
	private class DataServerUtilizationComparator implements Comparator<DataServerUtilization> {

		@Override
		public int compare(DataServerUtilization o1, DataServerUtilization o2) {
			if(o1.getDiskUtilization() < o2.getDiskUtilization()) {
				return -1;
			} else if (o1.getDiskUtilization() > o2.getDiskUtilization()) {
				return 1;
			}
			return o1.getDataServer() - o2.getDataServer();
		}
		
	}
}