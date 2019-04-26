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
	private static final Level sLogLevel = Level.OFF;
	private static Simulation simulation;


	/*
	Input Args sequence
	args[0] = WorkloadType
	args[1] = ClusterConfig.json
	args[2] = DataServerConfiguration.json
	args[3] = Workload.json (Optional)
	 */

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Incorrect input args sequence\n" +
					"\targs[0] = WorkloadType\n" +
					"\targs[1] = ClusterConfig.json\n" +
					"\targs[2] = DataServerConfiguration.json\n" +
					"\targs[3] = Workload.json (Optional). Exiting !!!");
			return;
		}
		String workloadType = args[0];
		String clusterConfig = args[1];
		String dataServerConfig = args[2];
		String workloadConfig = null;
		if (args.length == 4) {
			workloadConfig = args[3];
		}
		simulation = new Simulation();
		try {

			ClusterConfiguration clusterConfiguration = simulation.getClusterConfig(clusterConfig);

			List<DataserverConfiguration> dataserverConfigurations = simulation.getDataServerConfig(dataServerConfig);

			mDfs = DFSFactory.getInstance(clusterConfiguration.getmType(), clusterConfiguration, dataserverConfigurations);

			IClientWorkload workload = new WorkloadFactory().getWorkload(workloadType, mDfs, workloadConfig);
			workload.execute();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (mDfs != null) {
				mDfs.printStats();
			} else {
				System.out.println("The object DFS is null");
			}
		}


	}

	private ClusterConfiguration getClusterConfig(String clusterConfig) throws IOException {
		File file = simulation.getFileFromResources(clusterConfig);
		return ConfigUtils.getClusterConfig(file);
	}

	private List<DataserverConfiguration> getDataServerConfig(String dataServerConfig) throws IOException {
		return ConfigUtils.getDataServers
				(simulation.getFileFromResources(dataServerConfig));
	}

	private File getFileFromResources(String fileName) {

		return new File("resources/" + fileName);

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