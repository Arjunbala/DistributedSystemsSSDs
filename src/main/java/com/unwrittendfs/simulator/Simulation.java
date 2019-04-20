package com.unwrittendfs.simulator;

import com.unwrittendfs.simulator.client.workload.IClientWorkload;
import com.unwrittendfs.simulator.client.workload.WorkloadFactory;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.dfs.ClusterConfiguration;
import com.unwrittendfs.simulator.dfs.DFSFactory;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;
import com.unwrittendfs.simulator.utils.ConfigUtils;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

public class Simulation {
	
	private static Long sSimulatorTime = 0l;
	private static JSONParser jsonParser = new JSONParser();
	private static DistributedFileSystem mDfs;
	private static final Level sLogLevel = Level.INFO;
	private static Simulation simulation;
	
	public static void main(String[] args) throws IOException {
		if(args.length < 1){
			System.err.println("Workload type missing. Exiting !!!");
			return;
		}
		String workloadType = args[0];
		simulation = new Simulation();
		try {

			ClusterConfiguration clusterConfiguration = simulation.getClusterConfig();

			List<DataserverConfiguration> dataserverConfigurations = simulation.getDataServerConfig();

			mDfs = DFSFactory.getInstance(clusterConfiguration.getmType(), clusterConfiguration, dataserverConfigurations);

			IClientWorkload workload = new WorkloadFactory().getWorkload(workloadType, mDfs);
			workload.execute();
		} catch (Exception ex) {
			ex.printStackTrace();
			if (mDfs != null) {
				mDfs.printStats();
			}
		}


	}

	private ClusterConfiguration getClusterConfig() throws IOException {
		File file = simulation.getFileFromResources("ClusterConfig.json");
		return ConfigUtils.getClusterConfig(file);
	}

	private List<DataserverConfiguration> getDataServerConfig() throws IOException {
		return ConfigUtils.getDataServers
				(simulation.getFileFromResources("DataServerConfiguration.json"));
	}

	private File getFileFromResources(String fileName) {

		File file = new File("resources/" + fileName);

		if (file == null) {
			throw new IllegalArgumentException("file is not found!");
		} else {
			return file;
		}

	}

	
	public static Long getSimulatorTime() {
		return sSimulatorTime;
	}
	
	public static void updateSimulationTime(long time) {
		sSimulatorTime = time;
	}
	
	public static void incrementSimulatorTime() {
		sSimulatorTime++;
	}
	
	public static Level getLogLevel() {
		return sLogLevel;
	}
}