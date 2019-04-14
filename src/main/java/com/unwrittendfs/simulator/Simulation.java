package com.unwrittendfs.simulator;

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
import java.util.logging.Logger;

public class Simulation {
	
	private static int sSimulatorTime = 0;
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
		//List<Workload> workloads = simulation.fileCreationWorkLoad(10);
		mDfs = DFSFactory.getInstance(clusterConfiguration.getmType(), clusterConfiguration, dataserverConfigurations);
		int fd = mDfs.create("abc", 1);
		System.out.println(fd);
		String buffer = null;
		long written = mDfs.write(fd, buffer, 2048, 1);
		sSimulatorTime += 1;
		System.out.println(written);
		long offset = mDfs.seek(fd, 0, 1);
		sSimulatorTime += 1;
		long read = mDfs.read(fd, buffer, 1024, 1);
		sSimulatorTime += 1;
		System.out.println(read);
		read = mDfs.read(fd, buffer, 1024, 1);
		sSimulatorTime += 1;
		System.out.println(read);
		mDfs.delete(fd);
		sSimulatorTime += 1;
		read = mDfs.read(fd, buffer, 1024, 1);
		sSimulatorTime += 1;
		System.out.println(read);
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
	
	public static Level getLogLevel() {
		return sLogLevel;
	}
}