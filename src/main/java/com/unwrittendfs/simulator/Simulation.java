package com.unwrittendfs.simulator;

import com.unwrittendfs.simulator.client.workload.TestWorkload;
import com.unwrittendfs.simulator.client.workload.Workload;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.dfs.ClusterConfiguration;
import com.unwrittendfs.simulator.dfs.DFSFactory;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;
import com.unwrittendfs.simulator.utils.ConfigUtils;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class Simulation {
	
	private static Long sSimulatorTime = 0l;
	private static JSONParser jsonParser = new JSONParser();
	private static DistributedFileSystem mDfs;
	private static final Level sLogLevel = Level.INFO;
	
	public static void main(String args[]) throws IOException {

		Simulation simulation = new Simulation();
		File file = simulation.getFileFromResources("ClusterConfig.json");
		ClusterConfiguration clusterConfiguration = ConfigUtils.getClusterConfig(file);
		System.out.println(clusterConfiguration);
		List<DataserverConfiguration> dataserverConfigurations = ConfigUtils.getDataServers
				(simulation.getFileFromResources("DataServerConfiguration.json"));
		System.out.println(dataserverConfigurations);
		mDfs = DFSFactory.getInstance(clusterConfiguration.getmType(), clusterConfiguration, dataserverConfigurations);
		// TODO: Execute workload based on a workload factory
		new TestWorkload(mDfs).execute();
		mDfs.printStats();
		return;
	}

	private File getFileFromResources(String fileName) {

		File file = new File("resources/" + fileName);

		if (file == null) {
			throw new IllegalArgumentException("file is not found!");
		} else {
			return file;
		}

	}

	private List<Workload> fileCreationWorkLoad(int workloadCount){
		List<Workload> workloadList = new ArrayList<>();
		for (int i = 0;i < workloadCount;i++){
			Workload workload = new Workload();
			workload.setWorkloadId(i);
			workload.setClientId(i);
			workload.setWorkloadType(Workload.WorkloadType.CREATE);
			Map<String, Object> workloadData = new HashMap<>();
			workloadData.put("FILENAME" ,"abc.txt");
			workload.setWorkloadData(workloadData);
			workloadList.add(workload);
		}
		return workloadList;
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