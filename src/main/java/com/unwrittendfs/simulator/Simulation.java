package com.unwrittendfs.simulator;

import com.unwrittendfs.simulator.client.workload.Workload;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.dfs.ClusterConfiguration;
import com.unwrittendfs.simulator.utils.ConfigUtils;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Simulation {
	
	private static int sSimulatorTime = 0;
	private static JSONParser jsonParser = new JSONParser();
	
	public static void main(String args[]) throws IOException {

		Simulation simulation = new Simulation();
		File file = simulation.getFileFromResources("ClusterConfig.json");
		ClusterConfiguration clusterConfiguration = ConfigUtils.getClusterConfig(file);
		System.out.println(clusterConfiguration);
		List<DataserverConfiguration> dataserverConfigurations = ConfigUtils.getDataServers
				(simulation.getFileFromResources("DataServerConfiguration.json"));
		System.out.println(dataserverConfigurations);
		List<Workload> workloads = simulation.fileCreationWorkLoad(10);

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

	
	public static int getSimulatorTime() {
		return sSimulatorTime;
	}
}