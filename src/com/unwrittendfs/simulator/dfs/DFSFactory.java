package com.unwrittendfs.simulator.dfs;

import java.util.List;

import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;

public class DFSFactory {
	public static DistributedFileSystem getInstance(String type, ClusterConfiguration cluster_config,
			List<DataserverConfiguration> data_server_configs) {
		switch(type) {
		case "GFS":
			return new GoogleFileSystem(cluster_config, data_server_configs);
		case "Ceph":
			return new CephFileSystem(cluster_config, data_server_configs);
			
		}
		return null;
	}
}